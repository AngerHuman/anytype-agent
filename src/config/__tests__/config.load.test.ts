import { describe, expect, it } from "bun:test";
import { loadFromEnv } from "../config.load";

const minimal = {
  ANYTYPE__API_URL: "http://127.0.0.1:31012",
  ANYTYPE__BOT_NAME: "DevBot",
  ANYTYPE__API_KEY: "secret_token",
  LLM__0__MODE: "cli",
  LLM__0__CLI: "claude",
};

describe("loadFromEnv (dotted env -> AppConfig)", () => {
  it("unflattens dotted keys into the nested shape", () => {
    const config = loadFromEnv(minimal);

    expect(config.ANYTYPE).toEqual({
      API_URL: "http://127.0.0.1:31012",
      BOT_NAME: "DevBot",
      API_KEY: "secret_token",
      API_VERSION: "2025-11-08",
    });
    expect(config.LLM).toEqual([{ NAME: "default", MODE: "cli", CLI: "claude" }]);
  });

  it("materializes nested defaults when SETTINGS is absent", () => {
    expect(loadFromEnv(minimal).SETTINGS).toEqual({
      DEBOUNCE_MS: 800,
      RETRY_DELAY_MS: 3000,
      SCAN_INTERVAL_MS: 60000,
      MAX_ACTIVE_CHATS: 50,
    });
  });

  it("applies the PROXY_PORT default", () => {
    expect(loadFromEnv(minimal).PROXY_PORT).toBe(31013);
  });

  it("coerces string env values to numbers", () => {
    const config = loadFromEnv({ ...minimal, SETTINGS__DEBOUNCE_MS: "1234" });
    expect(config.SETTINGS.DEBOUNCE_MS).toBe(1234);
  });

  it("supports multiple indexed providers", () => {
    const config = loadFromEnv({
      ...minimal,
      LLM__0__CLI: "agy",
      LLM__1__MODE: "api",
      LLM__1__API_URL: "https://api.openai.com/v1",
      LLM__1__API_KEY: "sk-test",
      LLM__1__MODEL: "gpt-4o",
    });

    expect(config.LLM).toHaveLength(2);
    const [cli, api] = config.LLM;
    expect(cli?.MODE).toBe("cli");
    expect(api?.MODE).toBe("api");
  });

  it("wraps a lone provider written without an index into an array", () => {
    const config = loadFromEnv({
      ANYTYPE__API_URL: "http://127.0.0.1:31012",
      ANYTYPE__BOT_NAME: "DevBot",
      ANYTYPE__API_KEY: "secret_token",
      LLM__MODE: "api",
      LLM__API_URL: "https://api.openai.com/v1",
      LLM__API_KEY: "sk-test",
      LLM__MODEL: "gpt-4o",
    });

    expect(config.LLM).toEqual([
      {
        NAME: "default",
        MODE: "api",
        API_URL: "https://api.openai.com/v1",
        API_KEY: "sk-test",
        MODEL: "gpt-4o",
      },
    ]);
  });

  it("fails with a readable field list when required vars are missing", () => {
    expect(() => loadFromEnv({})).toThrow(/Invalid environment variables/);
    expect(() => loadFromEnv({})).toThrow(/ANYTYPE/);
  });
});
