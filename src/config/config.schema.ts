import Type, { type Static } from "typebox";
import { Settings as TypeboxSettings } from "typebox/system";

TypeboxSettings.Set({ correctiveParse: true });

export const AnytypeConfig = Type.Object({
  API_URL: Type.String({ minLength: 1 }),
  API_KEY: Type.String({ minLength: 1 }),
  BOT_NAME: Type.String({ minLength: 1 }),
});
export type AnytypeConfig = Static<typeof AnytypeConfig>;

// `default: {}` is required: Value.Default does not materialize an absent required
// nested object, so without it a config that omits SETTINGS fails to parse instead
// of falling back to the per-field defaults below.
export const AnytypeSettings = Type.Object(
  {
    DEBOUNCE_MS: Type.Number({ default: 800 }),
    RETRY_DELAY_MS: Type.Number({ default: 3000 }),
    SCAN_INTERVAL_MS: Type.Number({ default: 60000 }),
    MAX_ACTIVE_CHATS: Type.Number({ default: 50 }),
  },
  { default: {} },
);
export type AnytypeSettings = Static<typeof AnytypeSettings>;

export const LlmCli = Type.Object({
  NAME: Type.String({ minLength: 1, default: "default" }),
  MODE: Type.Literal("cli"),
  CLI: Type.String({ minLength: 1 }),
  MODEL: Type.Optional(Type.String({ minLength: 1 })),
  SSH: Type.Optional(Type.String({ minLength: 1 })),
  SSH_KEY: Type.Optional(Type.String({ minLength: 1 })),
});
export type LlmCli = Static<typeof LlmCli>;

export const LlmApi = Type.Object({
  NAME: Type.String({ minLength: 1, default: "default" }),
  MODE: Type.Literal("api"),
  API_URL: Type.String({ minLength: 1 }),
  API_KEY: Type.String({ minLength: 1 }),
  MODEL: Type.String({ minLength: 1 }),
});
export type LlmApi = Static<typeof LlmApi>;

export const AppConfig = Type.Object({
  ANYTYPE: AnytypeConfig,
  SETTINGS: AnytypeSettings,
  LLM: Type.Array(Type.Union([LlmCli, LlmApi])),
  /**
   * @deprecated Requested loopback port for the proxy, and the port the host agent is
   * told to reach it on. Overridable so tests can pass 0 and bind an OS-assigned free
   * port instead of colliding on a fixed one.
   *
   * TODO: replace with a runtime port negotiated over an SSH reverse port forward.
   */
  PROXY_PORT: Type.Number({ default: 31013, deprecated: true }),
});
export type AppConfig = Static<typeof AppConfig>;

export interface AppConfigRaw {
  ANYTYPE?: AnytypeConfig;
  SETTINGS?: AnytypeSettings;
  LLM?: (LlmCli | LlmApi)[] | LlmCli | LlmApi;
  PROXY_PORT?: number;
}
