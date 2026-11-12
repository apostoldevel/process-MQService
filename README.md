[![ru](https://img.shields.io/badge/lang-ru-green.svg)](README.ru-RU.md)

MQService — message exchange
-

**Process** for **Apostol CRM**[^crm].

Description
-

**MQService** is a background process-module for the [A-POST-OL (C++20)](https://github.com/apostoldevel/libapostol) framework. It carries a durable, store-and-forward message exchange between nodes — built for a star of a shore data-centre and one or many vessels connected over an unreliable, metered, weeks-apart link.

It is the replacement for the withdrawn `Replication` process. The row-by-row path was rejected: it applied inbound rows with raw DML after disabling triggers, bypassing the entity layer and its hash chain. MQService applies inbound only through each message class's `ingest` — which checks the signature, the origin and the place in the chain, and parks what fails in dead-letter instead of writing it anyway.

The model, in one line: a **Kafka-style durable log** (read does not consume; the reader keeps the offset), **RabbitMQ-style routing** (a class of event binds to a channel; priority lives on the channel), and **MQTT-style per-class delivery** (priority · delivery guarantee · time-to-live are three independent axes, not one system-wide setting) — carried over a **short catch-up session**, not a live socket. The full rationale lives in the consuming project's design note.

> ⚠️ **State — skeleton (this repository's first milestone).** What works today: the process lifecycle, its own database role, service authentication, and runtime control over `LISTEN mq_cmd`. What is **not** implemented yet: the exchange session itself (batch collection, the round-trip, applying inbound, confirming watermarks) — it is defined by the `mq` schema and lands as a separate step. `run_session()` is a stub that logs and touches no database. There is **no raw DML anywhere**, by design.

Main characteristics:

* Written in C++20 on the asynchronous, non-blocking **epoll** model.
* Connects to **PostgreSQL** through `libpq` under a **database role of its own** (`postgres.exchange`), on a pool it opens itself — not the process's helper pool. The database can then admit inbound exchange under that role alone.
* Authenticates via OAuth2 `client_credentials` with `BotSession`.
* Takes runtime commands over a PostgreSQL `LISTEN "mq_cmd"` channel — **applied without a restart**: on a vessel a process restart is an event, and changing an interval or pausing a channel must not be one.

### Architecture

MQService follows the **ProcessModule** pattern:

```
Application
  └── ModuleProcess (generic process shell: signals, EventLoop, PgPool)
        └── MQService (ProcessModule: business logic only)
```

The process lifecycle (signal handling, crash recovery, the heartbeat timer) is owned by the generic `ModuleProcess` shell. The framework beats and stops only the pools it opened, so `MQService` does that for its own pool: a pool timer every 60 s, `on_stopping` cancels it and mutes the pool's notifications, `busy` waits for the work in flight (once a round failing a query with no connection), `on_stop` detaches the pool and destroys it while the loop lives. The service session is not closed at stop — it outlives the process by design and is handed back on the next start.

### How it works (today)

```
heartbeat (1 s)
  └── BotSession::refresh_if_needed()        (own pool: its own 60 s timer)
  └── once authenticated → status: running
  └── process_command_queue()          ← drains commands from LISTEN "mq_cmd"
  └── mode == paused   → hold, nothing runs
      mode == manual   → run_session() only on an explicit "sync now"
      mode == automatic→ run_session() on the interval, or on "sync now"
        └── run_session()  ← STUB: logs, no exchange yet (arrives with the mq schema)
```

### Run mode — changed on the fly, never by a restart

| Mode | Session runs |
|------|--------------|
| `automatic` | on the configured interval, and on an explicit request |
| `manual` | only on an explicit "sync now" |
| `paused` | never; the channel is held until resumed |

### Commands on `LISTEN "mq_cmd"`

The `NOTIFY mq_cmd, '<payload>'` payload is either a JSON object or a bare shorthand word:

```
{"cmd":"mode","mode":"automatic|paused|manual"}   switch run mode
{"cmd":"sync"[,"channel":"<code>"]}               request an immediate session
"pause" | "resume" | "automatic" | "manual"       shorthand for a mode switch
"sync"                                            shorthand for a sync request
```

A malformed payload is logged and ignored; the process keeps running.

Configuration
-

In the application profile's `module` block:

```json
{
  "module": {
    "MQService": {
      "enable": false,
      "heartbeat": 60000,
      "mode": "automatic"
    }
  },
  "postgres": {
    "exchange": {
      "user": "exchange",
      "password": "${PGPASSWORD_EXCHANGE}"
    }
  }
}
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enable` | bool | `false` | Enable/disable the process. Keep it disabled until the `mq` schema and the exchange step are in place — enabled with no schema it has nothing to exchange. |
| `heartbeat` | int | `60000` | Session tick, in milliseconds. Administrator-tunable; the real per-band defaults live on `mq.channel`. |
| `mode` | string | `automatic` | Startup run mode: `automatic`, `paused` or `manual`. Overridden at runtime by `mq_cmd`. |
| `max_pending` | int | `256` | Cap on the `mq_cmd` command queue. A full queue drops the command with a warning rather than swallowing it — the queue carries control, so a lost command is logged. |

`postgres.exchange` — required when the process is enabled:

| Parameter | Type | Description |
|-----------|------|-------------|
| `user` | string | The database role MQService connects as. A role of its own: the role of the worker, helper or kernel pool is refused. |
| `password` | string | Its password, usually `${VAR}` expanded from the environment. |

Only these two keys (and `_comment`) are read. Every other connection parameter — host, port, database, TLS, `application_name` and the rest — is the helper pool's resolved connection — directly to the server, which `LISTEN` needs (a transaction pooler drops it). `PGUSER` / `PGPASSWORD` from the environment are **not** applied to this pool: they apply to every framework pool without a variable of its own and would silently replace the role.

The process **refuses to start**, naming the key, on: no `postgres.helper` connection, an absent section or one that is not an object, a key other than `user` / `password`, an empty value (an unset `${VAR}` expands to an empty string), a blank, a control or a non-ASCII character in `user`, a control character in `password`, a `user` equal to the role of another pool of the process (a pool that names no user is compared by libpq's default user). The master then respawns it with a backoff — the failure repeats in the log until the configuration is fixed.

The role must be able to do what the process does on the database: sign in through `BotSession` (`api.login`, `api.get_sessions`, `api.authorize`), `LISTEN`, and later the exchange calls. With db-platform the simplest grant is membership in the helper pool's role. Checks in the database that name the connection's role decide by that name, not by membership — what the database admits under the role is the database's configuration.

**Logging.** The own pool writes to stderr at `log.level` — what the framework's pools do when `log.postgres` is not set. It does not write to `log.postgres` when that is set: the framework's file logger is not reachable from a module.

Also required:

* the `postgres.helper` connection (its address is the one the own pool uses);
* OAuth2 `service` credentials in `conf/oauth2/default.json`.

Installation
-

Follow the build and installation guidance for [A-POST-OL (C++20)](https://github.com/apostoldevel/libapostol#build-and-installation).

[^crm]: **Apostol CRM** — a template project built on the [A-POST-OL](https://github.com/apostoldevel/libapostol) (C++20) and [PostgreSQL Framework for Backend Development](https://github.com/apostoldevel/db-platform) frameworks.
