#ifdef WITH_POSTGRESQL

#include "MQService/MQService.hpp"

#include "apostol/application.hpp"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <cctype>

namespace apostol
{

// ─── mode helpers ────────────────────────────────────────────────────────────

const char* MQService::mode_name(Mode m) noexcept
{
    switch (m) {
        case Mode::automatic: return "automatic";
        case Mode::paused:    return "paused";
        case Mode::manual:    return "manual";
    }
    return "automatic";
}

bool MQService::parse_mode(std::string_view s, Mode& out) noexcept
{
    if (s == "automatic" || s == "auto" || s == "resume") { out = Mode::automatic; return true; }
    if (s == "paused"    || s == "pause")                 { out = Mode::paused;    return true; }
    if (s == "manual")                                    { out = Mode::manual;    return true; }
    return false;
}

// ─── on_start ────────────────────────────────────────────────────────────────

void MQService::on_start(EventLoop& loop, Application& app)
{
    pool_   = &app.db_pool();
    logger_ = &app.logger();
    loop_   = &loop;

    // apibot authentication (OAuth2 client_credentials), as the other processes do.
    bot_ = std::make_unique<BotSession>(*pool_, "MQService/2.0", "127.0.0.1");

    auto [client_id, client_secret] = app.providers().credentials("service");
    if (!client_id.empty())
        bot_->set_credentials(std::move(client_id), std::move(client_secret));

    load_config(app);

    // Deliberate: the first session fires on the first heartbeat after the bot
    // authenticates, not one interval later. A node that has just come up — on a
    // vessel, after a restart that is itself an event — should catch up at once,
    // not sit a full interval behind. Leaving next_tick_ at the epoch would give
    // the same effect by accident; setting it here makes it a decision.
    next_tick_ = std::chrono::system_clock::now();

    // Runtime control channel. The callback only enqueues; commands are applied
    // in the heartbeat, off the notify dispatch path.
    pool_->listen("mq_cmd", [this](std::string_view /*channel*/, std::string_view payload) {
        on_command(payload);
    });

    logger_->notice("MQService started (mode={}, interval={}ms) — skeleton: exchange session not implemented yet",
                    mode_name(mode_), interval_.count());
}

// ─── heartbeat ───────────────────────────────────────────────────────────────

void MQService::heartbeat(std::chrono::system_clock::time_point now)
{
    if (!bot_ || !pool_)
        return;

    bot_->refresh_if_needed();

    if (status_ == Status::stopped) {
        if (bot_->valid())
            status_ = Status::running;
        return;
    }

    // Commands are honoured in every mode — including a switch out of "paused".
    process_command_queue();

    if (mode_ == Mode::paused) {
        // Held. An explicit request made while paused is dropped, not deferred:
        // the operator has stopped the channel, so acting on a stale "sync now"
        // the moment they resume would be surprising. They can re-issue it.
        //
        // notice, not debug: the deployed profiles run at "info", and this line
        // is the only answer to "why did my sync do nothing?" — it has to be
        // audible at the level the process actually runs.
        if (sync_requested_) {
            logger_->notice("MQService: sync request ignored while paused");
            sync_requested_ = false;
        }
        return;
    }

    const bool tick_due = (mode_ == Mode::automatic && now >= next_tick_);

    if (tick_due || sync_requested_) {
        sync_requested_ = false;
        run_session();
        next_tick_ = now + interval_;
    }
}

// ─── on_stop ─────────────────────────────────────────────────────────────────

void MQService::on_stop()
{
    if (pool_)
        pool_->unlisten("mq_cmd");
    if (bot_)
        bot_->sign_out();
    bot_.reset();
}

// ─── on_command / process_command_queue ──────────────────────────────────────
//
// NOTIFY "mq_cmd" payload, either a JSON object or a bare shorthand word:
//   {"cmd":"mode","mode":"automatic|paused|manual"}   switch run mode
//   {"cmd":"sync"[,"channel":"<code>"]}               request an immediate session
//   "pause" | "resume" | "automatic" | "manual"       shorthand for a mode switch
//   "sync"                                            shorthand for a sync request
//

void MQService::on_command(std::string_view payload)
{
    std::string cmd(payload);
    if (cmd.empty())
        return;

    // The queue carries control, not data: a dropped command is a lost "resume"
    // that leaves the channel paused forever. So a full queue is reported, not
    // swallowed. Logging here is safe — the notify callback logs directly, as
    // ReportServer::on_notify does. (Reaching max_pending is itself abnormal.)
    if (pending_cmds_.size() >= max_pending_) {
        if (logger_)
            logger_->warn("MQService: command queue full ({}), dropped '{}'",
                          max_pending_, cmd);
        return;
    }

    pending_cmds_.push_back(std::move(cmd));
}

void MQService::process_command_queue()
{
    if (pending_cmds_.empty())
        return;

    auto pending = std::move(pending_cmds_);
    pending_cmds_.clear();

    for (const auto& cmd : pending)
        apply_command(cmd);
}

void MQService::apply_command(std::string_view payload)
{
    // Trim surrounding whitespace for the shorthand form.
    std::size_t b = 0, e = payload.size();
    while (b < e && std::isspace(static_cast<unsigned char>(payload[b]))) ++b;
    while (e > b && std::isspace(static_cast<unsigned char>(payload[e - 1]))) --e;
    std::string_view trimmed = payload.substr(b, e - b);

    if (trimmed.empty())
        return;

    // Shorthand: a bare word that is not JSON.
    if (trimmed.front() != '{') {
        Mode m;
        if (parse_mode(trimmed, m)) {
            if (m != mode_) {
                logger_->notice("MQService: mode {} → {}", mode_name(mode_), mode_name(m));
                mode_ = m;
            }
        } else if (trimmed == "sync") {
            logger_->notice("MQService: sync requested (all channels)");
            sync_requested_ = true;
        } else {
            logger_->warn("MQService: unknown command '{}'", std::string(trimmed));
        }
        return;
    }

    // JSON form.
    nlohmann::json j;
    try {
        j = nlohmann::json::parse(trimmed);
    } catch (const std::exception& ex) {
        logger_->warn("MQService: malformed mq_cmd payload: {}", ex.what());
        return;
    }

    const std::string cmd = j.value("cmd", std::string{});

    if (cmd == "mode") {
        Mode m;
        if (parse_mode(j.value("mode", std::string{}), m)) {
            if (m != mode_) {
                logger_->notice("MQService: mode {} → {}", mode_name(mode_), mode_name(m));
                mode_ = m;
            }
        } else {
            logger_->warn("MQService: mode command with unknown mode '{}'",
                          j.value("mode", std::string{}));
        }
    } else if (cmd == "sync") {
        const std::string channel = j.value("channel", std::string{});
        if (channel.empty())
            logger_->notice("MQService: sync requested (all channels)");
        else
            logger_->notice("MQService: sync requested (channel={})", channel);
        // The channel scope is carried by the exchange card; the skeleton only
        // records that a session was asked for.
        sync_requested_ = true;
    } else {
        logger_->warn("MQService: unknown mq_cmd '{}'", cmd);
    }
}

// ─── run_session ─────────────────────────────────────────────────────────────
//
// Stub. The real session — collect a batch by channel with a priority filter,
// one compressed HTTP round-trip, apply inbound through each class's `ingest`,
// confirm watermarks moved by the RECEIVER on receipt — arrives with the mq
// schema (T078) as a separate card. Nothing here touches the database: the
// schema does not exist yet, and there is to be no raw DML at any point.
//

void MQService::run_session()
{
    logger_->debug("MQService: session tick (mode={}) — exchange not implemented yet, nothing to do",
                   mode_name(mode_));
}

// ─── load_config ─────────────────────────────────────────────────────────────

void MQService::load_config(Application& app)
{
    auto* cfg = app.module_config("MQService");
    if (!cfg)
        return;

    if (cfg->contains("heartbeat") && (*cfg)["heartbeat"].is_number()) {
        auto ms = (*cfg)["heartbeat"].get<long long>();
        if (ms > 0)
            interval_ = milliseconds(ms);
    }

    // Same knob as ReportServer's max_pending — the command-queue cap.
    if (cfg->contains("max_pending") && (*cfg)["max_pending"].is_number()) {
        auto n = (*cfg)["max_pending"].get<long long>();
        if (n > 0)
            max_pending_ = static_cast<std::size_t>(n);
    }

    if (cfg->contains("mode") && (*cfg)["mode"].is_string()) {
        Mode m;
        if (parse_mode((*cfg)["mode"].get<std::string>(), m))
            mode_ = m;
        else
            logger_->warn("MQService: config mode '{}' unknown, keeping {}",
                          (*cfg)["mode"].get<std::string>(), mode_name(mode_));
    }
}

} // namespace apostol

#endif // WITH_POSTGRESQL
