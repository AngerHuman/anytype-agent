import "reflect-metadata";
import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import { Test } from "@nestjs/testing";
import { AnytypeService } from "../client/anytype.service";
import { makeAppEnv } from "../config/__tests__/fixtures";
import { APP_CONFIG } from "../config/config.module";
import { LlmService } from "../llm/llm.service";
import { SshService } from "../ssh/ssh.service";

/**
 * Boot smoke test: `tsc` cannot verify Nest's DI wiring (injection is resolved at runtime
 * from `@Inject`/`emitDecoratorMetadata`), so this is the only guard that the provider
 * graph actually composes and the app starts.
 */
describe("AppModule boots (DI wiring)", () => {
  let fetchSpy: ReturnType<typeof spyOn>;
  let sshSpy: ReturnType<typeof spyOn>;
  const originalEnv = { ...process.env };

  beforeAll(async () => {
    Object.assign(
      process.env,
      makeAppEnv({ ANYTYPE__BOT_NAME: "NestBot", ANYTYPE__API_KEY: "nest_key_999" }),
    );
    // Bootstrap reaches the network (Anytype healthcheck) and the host (SSH `--help`).
    fetchSpy = spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ data: [] }), { status: 200 }),
    );
    sshSpy = spyOn(SshService.prototype, "connect").mockResolvedValue({} as any);
  });

  afterAll(() => {
    fetchSpy?.mockRestore();
    sshSpy?.mockRestore();
    process.env = originalEnv;
  });

  it("resolves the full DI graph at bootstrap", async () => {
    const { AppModule } = await import("../app.module");

    const moduleRef = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();

    await moduleRef.init();

    expect(moduleRef.get(APP_CONFIG)).toBeDefined();
    expect(moduleRef.get(AnytypeService)).toBeInstanceOf(AnytypeService);
    expect(moduleRef.get(LlmService)).toBeInstanceOf(LlmService);
    expect(sshSpy).toHaveBeenCalledTimes(1);

    await moduleRef.close();
  });
});
