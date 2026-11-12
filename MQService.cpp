#ifdef WITH_POSTGRESQL

#include "MQService/MQService.hpp"
#include "MQService/OwnPool.hpp"

#include "apostol/application.hpp"

#include <fmt/format.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <cctype>
#include <stdexcept>

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
    logger_ = &app.logger();
    loop_   = &loop;

    // The process's own login, from configuration alone and before anything is
    // built: a missing or wrong postgres.exchange is refused here, by name —
    // not later by the database, at every call, as a bare access error.
    std::string own_conninfo;
    std::string own_role;
    {
        const auto& root = app.config().json();
        nlohmann::json section;
        if (const auto pg = root.find("postgres"); pg != root.end() && pg->is_object() && pg->contains("exchange"))
            section = pg->at("exchange");
        try {
            const auto& st = app.settings();
            own_conninfo = mqservice::own_conninfo(st.pg_conninfo_helper, section, "postgres.exchange",
                                                   {st.pg_conninfo_worker, st.pg_conninfo_kernel});
        } catch (const std::exception& e) {
            throw std::runtime_error(fmt::format(
                "MQService: {} — the exchange connects as a database role of its own, never the helper's; "
                "refusing to start", e.what()));
        }
        own_role = section.at("user").get<std::string>();
    }

    load_config(app);

    // The pool as the framework sets up a custom process's own: lazy, no
    // connection before the first query. From here on a throw takes the pool
    // with it — a PgPool outliving the loop it is registered with would be
    // destroyed into a dead loop.
    try {
        // Not the process's logger: its file target would put every pool line
        // (Connected, LISTEN, a notice per NOTIFY, a failed result) into the
        // error log. The framework's pools, without a postgres log, write to
        // stderr at log.level — this one does the same.
        pool_logger_ = std::make_unique<Logger>();
        pool_logger_->add_target(std::make_unique<StderrTarget>());
        pool_logger_->set_level(app.logger().level());

        const auto& st = app.settings();

        // The stop round, before the pool exists: finish_work's own formula
        // (libapostol, Application::finish_work) — a quarter of the stop budget
        // or of the time to the master's SIGKILL, whichever is shorter, at most
        // k_shutdown_drain — so that three rounds fit before either.
        const auto kill_after = std::chrono::duration_cast<milliseconds>(std::chrono::seconds(st.stop_timeout));
        round_ = std::min<milliseconds>(Application::k_shutdown_drain, std::min(app.stop_budget(), kill_after) / 4);

        own_pool_ = std::make_unique<PgPool>(loop, std::move(own_conninfo),
            static_cast<std::size_t>(st.pg_pool_min), static_cast<std::size_t>(st.pg_pool_max), pool_logger_.get());
        own_pool_->set_reset(st.pg_reset);
        own_pool_->set_stall_timeout(std::chrono::seconds(st.pg_stall_timeout));
        own_pool_->set_idle_timeout(std::chrono::seconds(st.pg_pool_idle_timeout));
        own_pool_->set_lifetime(std::chrono::seconds(st.pg_pool_lifetime));
        own_pool_->set_lazy(true);
        own_pool_->start();

        // Every minute, as the framework's own pool timer: health, the minimum,
        // idle connections, the recovery of the LISTEN connection. A timer of its
        // own, not the module heartbeat: that one is gated (process.leader) and
        // returns early before the service session exists.
        pool_timer_ = loop.add_timer(std::chrono::duration_cast<std::chrono::milliseconds>(k_pool_beat),
                                     [this] { if (own_pool_) own_pool_->heartbeat(); });

        // Service authentication (OAuth2 client_credentials), on the own pool.
        bot_ = std::make_unique<BotSession>(*own_pool_, "MQService/2.0", "127.0.0.1");

        auto [client_id, client_secret] = app.providers().credentials("service");
        if (!client_id.empty())
            bot_->set_credentials(std::move(client_id), std::move(client_secret));

        // Deliberate: the first session fires on the first heartbeat after the bot
        // authenticates, not one interval later. A node that has just come up — on a
        // vessel, after a restart that is itself an event — should catch up at once,
        // not sit a full interval behind. Leaving next_tick_ at the epoch would give
        // the same effect by accident; setting it here makes it a decision.
        next_tick_ = std::chrono::system_clock::now();

        // Runtime control channel. The callback only enqueues; commands are applied
        // in the heartbeat, off the notify dispatch path.
        own_pool_->listen("mq_cmd", [this](std::string_view /*channel*/, std::string_view payload) {
            on_command(payload);
        });

        logger_->notice("MQService started (mode={}, interval={}ms, database role '{}' from postgres.exchange) — "
                        "skeleton: exchange session not implemented yet",
                        mode_name(mode_), interval_.count(), own_role);
    } catch (...) {
        release_pool();
        throw;
    }
}

// ─── heartbeat ───────────────────────────────────────────────────────────────

void MQService::heartbeat(std::chrono::system_clock::time_point now)
{
    if (!bot_ || !own_pool_)
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

// ─── on_stopping / busy / on_stop ────────────────────────────────────────────
//
// The framework's stop, done here for the pool it did not open: in on_stopping
// the beat cancelled, notifications muted (the stop re-runs the loop) and the
// stop round started — a query with no connection is failed, as finish_work
// does for the framework's pools; busy() waits for the work in flight within
// the stop budget; then in on_stop — in the framework's own order — detach, the
// timers cancelled and the pool destroyed while the loop still lives. BotSession::sign_out() sends nothing: the service
// session outlives the process by design and is handed back on the next start.

void MQService::cancel_timer(int& id) noexcept
{
    if (id != EventLoop::kInvalidTimer && loop_) {
        try { loop_->cancel_timer(id); } catch (...) {}
    }
    id = EventLoop::kInvalidTimer;
}

void MQService::release_pool() noexcept
{
    cancel_timer(pool_timer_);
    cancel_timer(stop_timer_);
    bot_.reset();
    own_pool_.reset();
    pool_logger_.reset();
}

void MQService::on_stopping()
{
    cancel_timer(pool_timer_);
    if (!own_pool_)
        return;
    own_pool_->mute_listeners();

    // As finish_work does for the framework's pools, from the start of the stop:
    // every round, a query with nowhere to run — the database gone, no
    // connection up or connecting — is failed (PgPool::fail_stranded: waiting,
    // an attempt, failing — one step a call). A timer of its own: its handlers
    // run in the loop's dispatch, and it wakes the loop when nothing else does.
    // What a failed query's handler throws stays here: out of the timer it
    // would end the stop's finish for the whole process. Cancelled in on_stop.
    cancel_timer(stop_timer_);
    try {
        stop_timer_ = loop_->add_timer(round_, [this] {
            if (!own_pool_)
                return;
            try {
                const auto n = own_pool_->fail_stranded("no connection to the database, and the process is stopping",
                                                        round_);
                if (n > 0 && logger_)
                    logger_->warn("MQService: stop: {} database {} failed — no connection to the database",
                                  n, n == 1 ? "query" : "queries");
            } catch (const std::exception& e) {
                if (logger_)
                    logger_->warn("MQService: stop: failing queries with no connection: {}", e.what());
            } catch (...) {
            }
        });
    } catch (const std::exception& e) {
        if (logger_)
            logger_->warn("MQService: stop: queries with no connection will not be failed: {}", e.what());
    }
}

bool MQService::busy() const
{
    return own_pool_ && own_pool_->outstanding() > 0;
}

void MQService::on_stop()
{
    // Whatever happens before it, the pool is destroyed here, while the loop
    // lives: after on_stop nothing of the framework stops a pool it did not open.
    try {
        if (own_pool_) {
            own_pool_->unlisten("mq_cmd");
            own_pool_->detach();   // a late result is not delivered to what is released below
        }
        if (bot_)
            bot_->sign_out();
    } catch (const std::exception& e) {
        if (logger_)
            logger_->warn("MQService: stop: {}", e.what());
    } catch (...) {
    }
    release_pool();
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
        // The channel scope is carried by the exchange step; the skeleton only
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
// schema as a separate step. Nothing here touches the database: the
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
