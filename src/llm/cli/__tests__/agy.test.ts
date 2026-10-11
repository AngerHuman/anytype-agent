import { describe, expect, it, mock } from "bun:test";
import type { ModelMessage } from "ai";
import { firstValueFrom, Subject, toArray } from "rxjs";
import {
  createMockAliasHandler,
  createMockSshClient,
  createTestRunContext,
  mockApiParams,
  mockCliConfig,
} from "../../__tests__/test.fixtures";
import { AgyCliProvider } from "../agy";

describe("AgyCliProvider (Stream & Process Unit Tests)", () => {
  const setupProvider = (stdout$: Subject<string>, onKill = mock(() => {})) => {
    const mockSsh = createMockSshClient(stdout$, onKill);
    const mockAliasHandler = createMockAliasHandler();

    const provider = new AgyCliProvider(
      mockSsh.client,
      mockCliConfig,
      mockAliasHandler,
      mockApiParams,
      "agy",
    );

    return {
      provider,
      mockSsh,
      mockAliasHandler,
      getSpawnOpts: mockSsh.getCapturedOpts,
    };
  };

  it("1. NDJSON chunking: handles lines split across multiple chunks", async () => {
    const stdout$ = new Subject<string>();
    const { provider } = setupProvider(stdout$);
    const messages: ModelMessage[] = [{ role: "user", content: "Hello" }];
    const ctx = createTestRunContext();

    const emissionsPromise = firstValueFrom(provider.exec(ctx, messages).pipe(toArray()));
    await Promise.resolve();

    stdout$.next('{"event": "step_update", "step_update": {"step_index": 0, "state": "ACTIVE", ');
    stdout$.next('"step_type": "tool", "tool_name": "search"}}\n{"event": "result", ');
    stdout$.next('"result": {"status": "SUCCESS", "response": "done"}}\n');
    stdout$.complete();

    const result = await emissionsPromise;

    expect(result).toHaveLength(2);
    expect(result[0]).toEqual({
      role: "assistant",
      content: [
        {
          type: "tool-call",
          toolCallId: "call_0",
          toolName: "search",
          input: {},
        },
      ],
    });
    expect(result[1]).toEqual({
      role: "assistant",
      content: "done",
    });
  });

  it("2. Tool Result mapping: maps DONE and ERROR tool steps to tool role messages", async () => {
    const stdout$ = new Subject<string>();
    const { provider } = setupProvider(stdout$);
    const emissionsPromise = firstValueFrom(
      provider.exec(createTestRunContext(), [{ role: "user", content: "test" }]).pipe(toArray()),
    );
    await Promise.resolve();

    // Done tool
    stdout$.next(
      `${JSON.stringify({
        event: "step_update",
        step_update: {
          step_index: 1,
          state: "DONE",
          step_type: "tool",
          tool_name: "fetch",
          tool_info: { output: "fetched text" },
        },
      })}\n`,
    );

    // Error tool
    stdout$.next(
      `${JSON.stringify({
        event: "step_update",
        step_update: {
          step_index: 2,
          state: "ERROR",
          step_type: "tool",
          tool_name: "fetch",
          tool_info: { error: { message: "Network error" } },
        },
      })}\n`,
    );

    // Result
    stdout$.next(
      `${JSON.stringify({
        event: "result",
        result: { status: "SUCCESS", response: "all done" },
      })}\n`,
    );
    stdout$.complete();

    const result = await emissionsPromise;
    expect(result).toHaveLength(3);
    expect(result[0]).toEqual({
      role: "tool",
      content: [
        {
          type: "tool-result",
          toolCallId: "call_1",
          toolName: "fetch",
          output: { type: "text", value: "fetched text" },
        },
      ],
    });
    expect(result[1]).toEqual({
      role: "tool",
      content: [
        {
          type: "tool-result",
          toolCallId: "call_2",
          toolName: "fetch",
          output: { type: "error-text", value: "Network error" },
        },
      ],
    });
    expect(result[2]).toEqual({
      role: "assistant",
      content: "all done",
    });
  });

  it("3. Non-JSON lines and init events: quietly ignored without dropping stream", async () => {
    const stdout$ = new Subject<string>();
    const { provider } = setupProvider(stdout$);
    const emissionsPromise = firstValueFrom(
      provider.exec(createTestRunContext(), [{ role: "user", content: "hi" }]).pipe(toArray()),
    );
    await Promise.resolve();

    stdout$.next("Warning: SSH key not found in cache\n");
    stdout$.next(`${JSON.stringify({ event: "init", conversation_id: "conv-123" })}\n`);
    stdout$.next(
      `${JSON.stringify({
        event: "result",
        result: { status: "SUCCESS", response: "hello back" },
      })}\n`,
    );
    stdout$.complete();

    const result = await emissionsPromise;
    expect(result).toHaveLength(1);
    expect(result[0]).toEqual({
      role: "assistant",
      content: "hello back",
    });
  });

  it("4. Failure result: throws error into stream", async () => {
    const stdout$ = new Subject<string>();
    const { provider } = setupProvider(stdout$);
    const emissionsPromise = firstValueFrom(
      provider.exec(createTestRunContext(), [{ role: "user", content: "fail" }]).pipe(toArray()),
    );
    await Promise.resolve();

    stdout$.next(
      `${JSON.stringify({
        event: "result",
        result: { status: "FAILURE", error: "Token budget exceeded" },
      })}\n`,
    );
    stdout$.complete();

    expect(emissionsPromise).rejects.toThrow("agy execution failed: Token budget exceeded");
  });

  it("5. Unsubscribe: triggers local abort controller and cancels process", async () => {
    const stdout$ = new Subject<string>();
    const { provider, getSpawnOpts } = setupProvider(stdout$);
    const sub = provider
      .exec(createTestRunContext(), [{ role: "user", content: "long run" }])
      .subscribe();

    // Wait for spawn() promise to resolve and inner stdout$ subscription to be active
    await Promise.resolve();

    const spawnOpts = getSpawnOpts();
    expect(spawnOpts?.signal).toBeDefined();
    expect(spawnOpts?.signal?.aborted).toBe(false);

    // Unsubscribe from downstream while process is streaming
    sub.unsubscribe();

    expect(spawnOpts?.signal?.aborted).toBe(true);
  });

  it("6. Prompt serialization: serializes extended prompt with language guidelines into stdin", async () => {
    const stdout$ = new Subject<string>();
    const { provider, getSpawnOpts } = setupProvider(stdout$);
    const ctx = createTestRunContext();
    const messages: ModelMessage[] = [{ role: "user", content: "Привет" }];

    firstValueFrom(provider.exec(ctx, messages).pipe(toArray()));
    await Promise.resolve();

    const stdin = getSpawnOpts()?.stdin;
    expect(stdin).toBeDefined();
    expect(stdin).toContain("[SYSTEM]:");
    expect(stdin).toContain("Communication & Language Guidelines:");
    expect(stdin).toContain("[USER]:\nПривет");
  });
});
