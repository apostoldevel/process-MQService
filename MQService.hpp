#pragma once

#ifdef WITH_POSTGRESQL

#include "apostol/process_module.hpp"

#include "apostol/bot_session.hpp"
#include "apostol/logger.hpp"
#include "apostol/pg.hpp"

#include <chrono>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace apostol
{

class Application;
class EventLoop;

// ─── MQService ───────────────────────────────────────────────────────────────
//
// Background process module for ship ↔ shore message exchange — the replacement
// for the withdrawn Replication process (the row-by-row path was rejected
// because it wrote raw DML with triggers disabled, bypassing the entity layer —
// see process-Replication's own README).
//
// Model, in one line: a Kafka-style durable log with RabbitMQ-style routing and
// MQTT-style per-class delivery, carried over a short catch-up session rather
// than a live socket. The full rationale lives in the consuming project's
// design note.
//
// ── What this class is TODAY (skeleton) ──────────────────────────────────────
//
//   * lifecycle on libapostol's ProcessModule: on_start / heartbeat /
//     on_stopping / busy / on_stop;
//   * its own database role (postgres.exchange, OwnPool.hpp): the database
//     may admit inbound only under that role, so MQService does not run on the
//     process's helper pool; it opens, beats and stops a pool of its own;
//   * service authentication via BotSession (OAuth2 client_credentials), on
//     that pool;
//   * runtime control over a PostgreSQL LISTEN "mq_cmd" channel — mode
//     (automatic / paused / manual) and an explicit "sync now", both applied
//     WITHOUT a restart: on a vessel a process restart is an event, a change of
//     interval must not be one;
//   * a session tick that respects the mode.
//
// ── What this class is NOT yet ───────────────────────────────────────────────
//
//   * the exchange session itself — collecting a batch by channel, the single
//     compressed round-trip, applying inbound, confirming watermarks — is a
//     separate step after the `mq` schema lands, because its shape is
//     defined by that schema;
//   * therefore run_session() is a stub: it logs and does nothing. There is no
//     query against `mq.*` here (the schema does not exist yet) and there is
//     NO raw DML anywhere — inbound will later be applied through each class's
//     `ingest`, never by disabling triggers. That prohibition is the whole
//     reason the previous path was withdrawn.
//
// Configuration:
//   "module": { "MQService": {
//     "enable": false,                // until the mq schema and the exchange step
//     "heartbeat": 60000,             // session tick, ms — administrator-tunable
//     "mode": "automatic"             // automatic | paused | manual
//   } },
//   "postgres": { "exchange": {       // required when enabled; refused at start if not
//     "user": "exchange",             // a role of its own, not worker/helper/kernel
//     "password": "${PGPASSWORD_EXCHANGE}"
//   } }                               // host, port, database, TLS — the helper pool's
//
class MQService final : public ProcessModule
{
public:
    std::string_view name() const override { return "mq-service"; }

    void on_start(EventLoop& loop, Application& app) override;
    void heartbeat(std::chrono::system_clock::time_point now) override;
    void on_stopping() override;
    bool busy() const override;
    void on_stop() override;

private:
    using time_point   = std::chrono::system_clock::time_point;
    using milliseconds = std::chrono::milliseconds;

    // ── Run mode ─────────────────────────────────────────────────────────────
    //
    // Changed on the fly by an mq_cmd command, never by a restart.
    //   automatic — session runs on the interval, and on an explicit request;
    //   manual    — session runs ONLY on an explicit "sync now";
    //   paused    — nothing runs; the channel is held until resumed.
    //
    enum class Mode { automatic, paused, manual };

    enum class Status { stopped, running };

    // ── State ────────────────────────────────────────────────────────────────

    static constexpr auto k_pool_beat = std::chrono::seconds(60);          // as the framework's pool timer
    static constexpr auto k_round     = std::chrono::milliseconds(1'500);  // fail_stranded at stop: once a round, as the framework

    // The process's own pool, as role postgres.exchange.user (OwnPool.hpp) —
    // never the process's helper pool. The framework neither beats nor stops a
    // pool it did not open: pool_timer_, on_stopping(), busy() and on_stop()
    // do it here. Order matters: pool_logger_ outlives own_pool_ (~PgPool logs
    // into it), own_pool_ outlives bot_ (BotSession holds a reference to it).
    std::unique_ptr<Logger>  pool_logger_;   // stderr at log.level (README: Logging)
    std::unique_ptr<PgPool>  own_pool_;
    int                      pool_timer_{-1}; // EventLoop::TimerId; -1 = none

    mutable std::chrono::steady_clock::time_point next_round_{};   // busy(): the next fail_stranded

    Logger*     logger_{nullptr};   // borrowed from Application
    EventLoop*  loop_{nullptr};     // borrowed from Application

    std::unique_ptr<BotSession> bot_;

    Status status_{Status::stopped};
    Mode   mode_{Mode::automatic};

    time_point   next_tick_{};
    milliseconds interval_{60'000};   // 1 minute default; real defaults are per (channel, link) on mq.channel

    // An explicit "sync now" request, honoured on the next heartbeat.
    bool         sync_requested_{false};

    // ── mq_cmd intake ────────────────────────────────────────────────────────
    //
    // Commands arrive on LISTEN "mq_cmd". The notify callback only enqueues; the
    // queue is drained in the heartbeat, on the process's own event loop — the
    // pattern MessageServer uses for "outbox", so command handling never runs
    // inside libpq's notify dispatch.
    //
    std::vector<std::string> pending_cmds_;
    std::size_t              max_pending_{256};

    void on_command(std::string_view payload);
    void process_command_queue();
    void apply_command(std::string_view payload);

    // ── Session ──────────────────────────────────────────────────────────────

    void run_session();   // stub until the exchange step; see the class comment

    void load_config(Application& app);
    void cancel_pool_timer() noexcept;
    void release_pool() noexcept;

    static const char* mode_name(Mode m) noexcept;
    static bool         parse_mode(std::string_view s, Mode& out) noexcept;
};

} // namespace apostol

#endif // WITH_POSTGRESQL
