# Anytype Agent 🤖✨

Autonomous, reactive AI bridge connecting [Anytype](https://anytype.io) spaces and chats to a local
or remote AI agent.

Built with **Bun**, **NestJS 11**, **RxJS**, **TypeBox**, and the **Vercel AI SDK**.

[![ci](https://github.com/AngerHuman/anytype-agent/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/AngerHuman/anytype-agent/actions/workflows/ci.yml)

---

## 🚧 Status

| Area | Status | Notes |
| :--- | :---: | :--- |
| **Reactive chat observer** | ✅ Working | Per-chat SSE streams, debounce, bot-echo suppression, cancellation of a pending trigger when the bot already answered |
| **CLI providers** (`agy`, `opencode`) | ✅ Working | Executed over SSH on a remote host; prompts delivered via STDIN |
| **API provider** (OpenAI-compatible) | ✅ Working | Vercel AI SDK `streamText`, tools provided by the Anytype MCP server |
| **Space proxy with 6-digit alias** | ✅ Working | `Bun.serve` reverse proxy on an ephemeral port, reachable from the remote host through an SSH reverse forward |
| **Object mention polling** | ⏳ Not implemented | Mentions are only picked up from chat message streams |
| **Local CLI spawning** | ⏳ Not implemented | `cli` mode requires `LLM__SSH`. Without it the provider is dropped silently (the rejection is swallowed by `Promise.allSettled`) and the app dies at runtime with `No LLM provider available` |
| **Provider failover** | ⏳ Not implemented | `LlmService` uses the first provider in the list only (see `TODO` in `llm.service.ts`) |

---

## 🏗️ Architecture

```
                       ┌──────────────────────────────────────────┐
                       │             Anytype daemon               │
                       │        (HTTP REST & SSE, v2025-11-08)    │
                       └───────┬──────────────────────▲───────────┘
                               │                      │
                    ┌──────────▼─────────┐   ┌────────┴─────────────┐
                    │   AnytypeClient    │   │    AnytypeProxy      │
                    │ (fetch + SSE parse)│   │ (Bun.serve,127.0.0.1,│
                    └──────────┬─────────┘   │  ephemeral port,     │
                               │             │  6-digit space alias)│
                    ┌──────────▼─────────┐   └────────▲─────────────┘
                    │  AnytypeService    │            │ SSH reverse
                    │  (domain layer)    │            │ forward
                    └──────────┬─────────┘            │ (ssh2 forwardIn)
                               │                      │
                    ┌──────────▼─────────┐   ┌────────┴─────────────┐
                    │  ObserverService   │   │  Remote host CLI     │
                    │   & ChatsObserver  │   │  (agy / opencode)    │
                    └──────────┬─────────┘   └────────▲─────────────┘
                               │                      │ tools = HTTP
                    ┌──────────▼─────────┐            │ to the proxy
                    │     LlmService     ├────────────┘
                    └──────────┬─────────┘
                               │
          ┌────────────────────┼────────────────────┐
          ▼                    ▼                    ▼
  AgyCliProvider     OpenCodeCliProvider       LlmApiProvider
   (SSH + agy)         (SSH + opencode)     (AI SDK + AnytypeMcp)
```

`AnytypeMcp` is a stdio MCP client that spawns `bun run anytype-mcp` (shipped by the
`@anyproto/anytype-mcp` dependency) and exposes Anytype tools to the API provider.

---

## 📋 Prerequisites

- [Bun](https://bun.com) 1.x — or Docker + Docker Compose
- An Anytype daemon with the API enabled (e.g. `ghcr.io/anyproto/anytype-cli:latest`, or the
  desktop app with the local API turned on)
- **For `LLM__MODE=cli` only:** SSH access to a host that has the agent CLI (`agy` or `opencode`)
  installed, plus a private key
- **For `LLM__MODE=api`:** outbound HTTPS to an OpenAI-compatible endpoint. No SSH is needed.

---

## 🚀 Quick Start

### Bun, locally

```bash
bun install
cp .env.example .env      # then fill in ANYTYPE__* and one LLM__* provider
bun dev                   # watch mode, runs src/index.ts
```

Bun loads `.env` automatically, so no `dotenv` step is required.

### Docker Compose

```bash
cp .env.example .env      # fill in ANYTYPE__* and one LLM__* provider
docker compose up --build
```

`docker-compose.yaml` starts two services: `anytype-cli` (the Anytype daemon on `31012`) and
`anytype-agent` (Dockerfile target `dev`, `./src` bind-mounted for hot reload). The agent loads
`.env` through `env_file` and pins only four values, which always win over `.env`:
`ANYTYPE__API_URL=http://anytype-cli:31012`, `LLM__NAME=opencode-api`, `LLM__MODE=api`, and
`LLM__MODEL=glm-5.3-flash`. Everything else in `.env` (`SETTINGS__*`, `ANYTYPE__API_VERSION`, …) is
forwarded as-is, so the same file works for both Bun and Docker.

---

## ⚙️ Configuration

### Naming rules

Configuration is read from environment variables and is **nested**: `__` (double underscore) is the
path separator, because POSIX env names cannot contain dots.

| What | How to write it |
| :--- | :--- |
| `APP.ANYTYPE.API_URL` | `ANYTYPE__API_URL` |
| `APP.SETTINGS.DEBOUNCE_MS` | `SETTINGS__DEBOUNCE_MS` |
| First provider in the list | `LLM__NAME`, `LLM__MODE`, … |
| Explicit array index | `LLM__0__NAME`, `LLM__1__NAME`, … |

Config is validated on boot with TypeBox. Invalid or missing values fail fast with a readable field
list:

```
[Config] Invalid environment variables:
  - /: must have required properties ANYTYPE, LLM
```

### Anytype

| Variable | Description | Default |
| :--- | :--- | :--- |
| `ANYTYPE__API_URL` | Base URL of the Anytype HTTP API | **required** |
| `ANYTYPE__API_KEY` | API token (`Authorization: Bearer …`) | **required** |
| `ANYTYPE__BOT_NAME` | Name (or identity) of the bot member inside the spaces | **required** |
| `ANYTYPE__API_VERSION` | Value of the `Anytype-Version` header | `2025-11-08` |

The bot member must be `active` and have one of the roles `editor`, `admin`, or `owner`; an inactive
bot or a nameless (technical) space is skipped silently, while a read-only role logs a warning.

### Observer settings

| Variable | Description | Default |
| :--- | :--- | :--- |
| `SETTINGS__DEBOUNCE_MS` | Debounce window for message bursts | `800` |
| `SETTINGS__RETRY_DELAY_MS` | Delay between SSE reconnect attempts | `3000` |
| `SETTINGS__SCAN_INTERVAL_MS` | How often spaces and their chat lists are re-scanned | `60000` |
| `SETTINGS__MAX_ACTIVE_CHATS` | Declared in the schema, **currently unused** | `50` |

### LLM providers

`LLM` is an array. A single provider may be written without an index (`LLM__MODE=…`); multiple
providers need explicit indexes (`LLM__0__…`, `LLM__1__…`). Each entry is one of two shapes,
discriminated by `LLM__MODE`.

Common field:

| Variable | Description | Default |
| :--- | :--- | :--- |
| `LLM__NAME` | Provider label, used for logging | `default` |

#### Mode `cli` — agent CLI on a remote host

| Variable | Description |
| :--- | :--- |
| `LLM__MODE` | `cli` |
| `LLM__CLI` | Path to the CLI binary **on the remote host** (absolute recommended; the provider is picked from its basename) |
| `LLM__SSH` | `ssh://user@host:port` — the port is mandatory |
| `LLM__SSH_KEY` | Path to the private key (local file) |
| `LLM__MODEL` | Required for `opencode` (`run -m <model>`), ignored by `agy` |

The provider implementation is selected by the **basename** of `LLM__CLI`: `agy` → `AgyCliProvider`,
`opencode` → `OpenCodeCliProvider`. Anything else fails with `Unsupported CLI provider binary`.

Both CLIs are invoked over an SSH `exec` channel with the serialized prompt on STDIN and a 300 s
timeout. The system prompt points the CLI at `http://localhost:<port>`, where `<port>` is the remote
end of a reverse forward created by `ssh2.forwardIn` — that port is tunneled back to the agent's
in-process Anytype proxy.

```bash
LLM__NAME=agy-remote
LLM__MODE=cli
LLM__CLI=/Users/me/.local/bin/agy
LLM__SSH=ssh://me@host.docker.internal:22
LLM__SSH_KEY=/keys/id_ed25519
```

#### Mode `api` — OpenAI-compatible endpoint

| Variable | Description |
| :--- | :--- |
| `LLM__MODE` | `api` |
| `LLM__API_URL` | Base URL of the OpenAI-compatible API |
| `LLM__API_KEY` | API key |
| `LLM__MODEL` | Model name |

Tools come from the Anytype MCP server, which is started automatically (`bun run anytype-mcp` must
be resolvable — it is installed with the dependencies). Requests carry an
`x-opencode-session: <spaceId>:<chatId>` header.

```bash
LLM__NAME=opencode-api
LLM__MODE=api
LLM__API_URL=https://opencode.ai/zen/v1
LLM__API_KEY=...
LLM__MODEL=glm-5.3-flash
```

> Only the first successfully created provider is used per run. Configuring several providers is
> allowed and all of them are instantiated, but failover is not implemented yet.

> To run two providers at once, give each block its own index (`LLM__0__…`, `LLM__1__…`). Without
> indexes two blocks share one namespace and silently merge into a single provider.

### Deprecated / inert

| Variable | State |
| :--- | :--- |
| `PROXY_PORT` | Marked `deprecated` in the schema. The proxy binds an OS-assigned port (`port: 0`), so this value has no effect. |
| `SETTINGS__MAX_ACTIVE_CHATS` | Validated, but not read anywhere yet. |

---

## 🔄 What happens on a mention

1. `ObserverService` scans spaces every `SETTINGS__SCAN_INTERVAL_MS`, finds the bot member by name or
   identity, checks its role, and creates one `ChatsObserver` per space.
2. The observer lists non-archived chats, subscribes to each chat's SSE stream, and re-scans for new
   chats on the same interval. Dropped streams are retried after `SETTINGS__RETRY_DELAY_MS`.
3. Every `message_added` event is classified:
   - message from the bot itself → cancels the pending trigger if it replies to it (backfill safety);
   - direct mention of the bot → trigger;
   - reply to a message authored by the bot → trigger;
   - anything else → ignored.
4. Pending triggers are debounced (`SETTINGS__DEBOUNCE_MS`); a newer mention replaces the older one.
5. The agent posts an italic `⏳ Working...` message, then edits it in place with tool-call progress:
   `⏳ [Pending]` / `✅ [Done]` / `❌ [Error]` for a single tool call, and `⏳ [1/2]` / `✅ [2/2]` /
   `⚠️ [1/2]` / `❌ [0/2]` for several.
6. On success it posts the reply with `reply_to_message_id` pointing at the trigger, then deletes the
   progress message. On failure the progress message is rewritten to `⚠️ <error>`. If the run is
   preempted (a newer trigger arrives), the CLI process is killed and the progress message removed.
7. Tool-call history is kept in memory under the key `spaceId-chatId-botId-triggerId` and re-injected
   for earlier user messages, so follow-ups see what was already done. The map holds at most 100
   conversations and evicts the oldest key.

HTTP calls made through `ChatsObserver.request$` — chat history, replied-message lookup, and
progress create/edit/delete — time out after 15 s; the initial progress post gets 5 s and one retry.
The transport client itself sets no timeout, so space and chat scanning requests are not covered and
can hang indefinitely.

---

## 🧪 Development

| Command | What it does |
| :--- | :--- |
| `bun dev` | Watch mode (`bun --watch ./src/index.ts`) |
| `bun start` | Run once |
| `bun run test` | Run the test suite (12 files, without `.env`) |
| `bun run test:watch` | Tests in watch mode |
| `bun run lint` | `biome lint .` |
| `bun run format` | `biome format --write .` |
| `bun run check` | `biome check --write .` — **applies fixes** |
| `bun run ci` | `biome ci .` — check-only, no writes (what CI runs) |

A pre-commit hook (husky) runs `lint-staged` and `bun run test`.

> Use `bun run test`, not a bare `bun test`. Bun loads `.env` into the environment even under the
> test runner, so a local `LLM__API_URL` merges into the fixture's `LLM__0__…` provider and the
> suite fails on config validation. The `test` script passes `--no-env-file` to prevent that.

### CI / CD

- **`.github/workflows/ci.yml`** — runs on every `pull_request`, and is reusable via `workflow_call`:
  `bun install --frozen-lockfile` → `bun run ci` → `bun run test`.
- **`.github/workflows/deploy.yml`** — runs on push to `main`: calls `ci.yml` first (tests must pass),
  then builds and pushes a multi-arch image (`linux/amd64`, `linux/arm64`) to GHCR as
  `ghcr.io/<owner>/<repo>:latest` and `:sha-<short>`.
- `main` is protected by a ruleset: pull request required, one approving review, the `test` check
  must be green, squash merge only, no force-push, no deletion.

---

## 🐳 Docker

`Dockerfile` has four stages:

| Stage | Purpose |
| :--- | :--- |
| `base` | `oven/bun:1-debian` + `openssh-client` |
| `dev` | Full deps, used by `docker-compose` with `./src` mounted |
| `prod-deps` | Production dependencies only |
| `prod` | Runtime image for GHCR; runs `bun run start` |

Only `openssh-client` and `ca-certificates` are installed, so `cli` mode works out of the box; `api`
mode does not need SSH.

---

## 📄 License

MIT
