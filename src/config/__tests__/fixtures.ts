import type { AppConfig } from "../config.schema";

export type AppConfigOverrides = {
  ANYTYPE?: Partial<AppConfig["ANYTYPE"]>;
  SETTINGS?: Partial<AppConfig["SETTINGS"]>;
  LLM?: AppConfig["LLM"];
  PROXY_PORT?: number;
};

/**
 * Builds a fully-populated typed AppConfig for tests.
 *
 * Replaces the old `ConfigService` mocks: tests now inject a plain object, so no
 * test has to juggle dotted `process.env` keys or stringly-typed `config.get(...)`.
 */
export function makeAppConfig(overrides: AppConfigOverrides = {}): AppConfig {
  return {
    ANYTYPE: {
      API_URL: "http://127.0.0.1:31012",
      API_KEY: "test_api_key",
      BOT_NAME: "TestBot",
      ...overrides.ANYTYPE,
    },
    SETTINGS: {
      DEBOUNCE_MS: 10,
      RETRY_DELAY_MS: 10,
      SCAN_INTERVAL_MS: 60_000,
      MAX_ACTIVE_CHATS: 50,
      ...overrides.SETTINGS,
    },
    LLM: overrides.LLM ?? [
      {
        NAME: "test-cli",
        MODE: "cli",
        CLI: "claude",
        SSH: "ssh://testuser@host.docker.internal:22",
        SSH_KEY: "/keys/id_ed25519",
      },
    ],
    // 0 = OS-assigned free port, so tests never collide on a fixed proxy port.
    PROXY_PORT: overrides.PROXY_PORT ?? 0,
  };
}

/**
 * Dotted env keys matching the AppConfig schema, for tests that must bootstrap a
 * real application context (NestFactory has no provider overrides).
 */
export function makeAppEnv(overrides: Record<string, string> = {}): Record<string, string> {
  return {
    ANYTYPE__API_URL: "http://127.0.0.1:31012",
    ANYTYPE__API_KEY: "test_api_key",
    ANYTYPE__BOT_NAME: "TestBot",
    LLM__0__MODE: "cli",
    LLM__0__CLI: "claude",
    LLM__0__SSH: "ssh://testuser@host.docker.internal:22",
    LLM__0__SSH_KEY: "/keys/id_ed25519",
    // 0 = OS-assigned free port, avoids collisions when bootstrapping in tests.
    PROXY_PORT: "0",
    ...overrides,
  };
}
