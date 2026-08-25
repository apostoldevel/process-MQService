[![ru](https://img.shields.io/badge/lang-ru-green.svg)](README.ru-RU.md)

MQService — message exchange
-

**Process** for **Apostol CRM**[^crm].

Description
-

**MQService** is a background process-module for the [A-POST-OL (C++20)](https://github.com/apostoldevel/libapostol) framework. It carries a durable, store-and-forward message exchange between nodes — built for a star of a shore data-centre and one or many vessels connected over an unreliable, metered, weeks-apart link.

It is the replacement for the withdrawn `Replication` process. The row-by-row path was rejected: it applied inbound rows with raw DML after disabling triggers, bypassing the entity layer and its hash chain. MQService applies inbound only through each message class's `ingest` — which checks the signature, the origin and the place in the chain, and parks what fails in dead-letter instead of writing it anyway.

The model, in one line: a **Kafka-style durable log** (read does not consume; the reader keeps the offset), **RabbitMQ-style routing** (a class of event binds to a channel; priority lives on the channel), and **MQTT-style per-class delivery** (priority · delivery guarantee · time-to-live are three independent axes, not one system-wide setting) — carried over a **short catch-up session**, not a live socket. The full rationale lives in the consuming project's design note.

> ⚠️ **State — skeleton (this repository's first milestone).** What works today: the process lifecycle, apibot authentication, and runtime control over `LISTEN mq_cmd`. What is **not** implemented yet: the exchange session itself (batch collection, the round-trip, applying inbound, confirming watermarks) — it is defined by the `mq` schema and lands as a separate step. `run_session()` is a stub that logs and touches no database. There is **no raw DML anywhere**, by design.

Main characteristics:

* Written in C++20 on the asynchronous, non-blocking **epoll** model.
* Connects to **PostgreSQL** through `libpq` using the `apibot` role (helper connection pool).
* Authenticates via OAuth2 `client_credentials` with `BotSession`.
* Takes runtime commands over a PostgreSQL `LISTEN "mq_cmd"` channel — **applied without a restart**: on a vessel a process restart is an event, and changing an interval or pausing a channel must not be one.

### Architecture

MQService follows the **ProcessModule** pattern:

```
Application
  └── ModuleProcess (generic process shell: signals, EventLoop, PgPool)
        └── MQService (ProcessModule: business logic only)
```

The process lifecycle (signal handling, crash recovery, PgPool setup, the heartbeat timer) is owned by the generic `ModuleProcess` shell. `MQService` contains only the exchange logic.

### How it works (today)

```
heartbeat (1 s)
  └── BotSession::refresh_if_needed()
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
  }
}
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enable` | bool | `false` | Enable/disable the process. Keep it disabled until the `mq` schema and the exchange step are in place — enabled with no schema it has nothing to exchange. |
| `heartbeat` | int | `60000` | Session tick, in milliseconds. Administrator-tunable; the real per-band defaults live on `mq.channel`. |
| `mode` | string | `automatic` | Startup run mode: `automatic`, `paused` or `manual`. Overridden at runtime by `mq_cmd`. |
| `max_pending` | int | `256` | Cap on the `mq_cmd` command queue. A full queue drops the command with a warning rather than swallowing it — the queue carries control, so a lost command is logged. |

Also required:

* the `postgres.helper` connection string (used for the `apibot` pool);
* OAuth2 `service` credentials in `conf/oauth2/default.json`.

Installation
-

Follow the build and installation guidance for [A-POST-OL (C++20)](https://github.com/apostoldevel/libapostol#build-and-installation).

[^crm]: **Apostol CRM** — a template project built on the [A-POST-OL](https://github.com/apostoldevel/libapostol) (C++20) and [PostgreSQL Framework for Backend Development](https://github.com/apostoldevel/db-platform) frameworks.
