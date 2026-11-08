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
import { OpenCodeCliProvider } from "../opencode";

describe("OpenCodeCliProvider (Stream & Process Unit Tests)", () => {
  const setupProvider = (
    stdout$: Subject<string>,
    onKill = mock(() => {}),
    cliParams = { binaryPath: "opencode", model: "test-model" },
  ) => {
    const mockSsh = createMockSshClient(stdout$, onKill);
    const mockAliasHandler = createMockAliasHandler();

    const provider = new OpenCodeCliProvider(
      mockSsh.client,
      mockCliConfig,
      mockAliasHandler,
      mockApiParams,
      cliParams,
    );

    return {
      provider,
      mockSsh,
      mockAliasHandler,
      getSpawnOpts: mockSsh.getCapturedOpts,
    };
  };

  it("0. Throws error if model is not provided in CliParams", () => {
    const stdout$ = new Subject<string>();
    const mockSsh = createMockSshClient(stdout$);
    const mockAliasHandler = createMockAliasHandler();

    expect(
      () =>
        new OpenCodeCliProvider(mockSsh.client, mockCliConfig, mockAliasHandler, mockApiParams, {
          binaryPath: "opencode",
        }),
    ).toThrow("OpenCodeCliProvider requires a model to be explicitly configured");

    expect(
      () =>
        new OpenCodeCliProvider(mockSsh.client, mockCliConfig, mockAliasHandler, mockApiParams, {
          binaryPath: "opencode",
          model: "   ",
        }),
    ).toThrow("OpenCodeCliProvider requires a model to be explicitly configured");
  });

  it("1. NDJSON chunking: handles lines split across multiple chunks", async () => {
    const stdout$ = new Subject<string>();
    const { provider } = setupProvider(stdout$);
    const messages: ModelMessage[] = [{ role: "user", content: "Hello OpenCode" }];
    const ctx = createTestRunContext();

    const emissionsPromise = firstValueFrom(provider.exec(ctx, messages).pipe(toArray()));
    await Promise.resolve();

    stdout$.next(
      '{"type":"tool_use","timestamp":100,"sessionID":"ses_1","part":{"type":"tool","tool":"bash",',
    );
    stdout$.next(
      '"callID":"tool_123","state":{"status":"completed","input":{"command":"ls"},"output":"file1.txt"}}}\n',
    );
    stdout$.next(
      '{"type":"step_finish","timestamp":101,"sessionID":"ses_1","part":{"id":"prt_fin","messageID":"msg_1",',
    );
    stdout$.next('"sessionID":"ses_1","type":"step-finish","reason":"stop"}}\n');
    stdout$.complete();

    const result = await emissionsPromise;

    expect(result).toHaveLength(2);
    expect(result[0]).toEqual({
      role: "assistant",
      content: [
        {
          type: "tool-call",
          toolCallId: "tool_123",
          toolName: "bash",
          input: { command: "ls" },
        },
      ],
    });
    expect(result[1]).toEqual({
      role: "tool",
      content: [
        {
          type: "tool-result",
          toolCallId: "tool_123",
          toolName: "bash",
          output: { type: "text", value: "file1.txt" },
        },
      ],
    });
  });

  it("2. Tool Result mapping: maps completed and error tool results correctly", async () => {
    const stdout$ = new Subject<string>();
    const { provider } = setupProvider(stdout$);
    const emissionsPromise = firstValueFrom(
      provider.exec(createTestRunContext(), [{ role: "user", content: "test" }]).pipe(toArray()),
    );
    await Promise.resolve();

    stdout$.next(
      `${JSON.stringify({
        type: "tool_use",
        timestamp: 100,
        sessionID: "ses_1",
        part: {
          type: "tool",
          tool: "fetch",
          callID: "call_1",
          state: {
            status: "completed",
            input: { url: "http://example.com" },
            output: "fetched text",
          },
        },
      })}\n`,
    );

    stdout$.next(
      `${JSON.stringify({
        type: "tool_use",
        timestamp: 101,
        sessionID: "ses_1",
        part: {
          type: "tool",
          tool: "fetch",
          callID: "call_2",
          state: {
            status: "error",
            input: { url: "http://bad.local" },
            error: { message: "Network connection refused" },
          },
        },
      })}\n`,
    );

    stdout$.complete();

    const result = await emissionsPromise;
    expect(result).toHaveLength(4);
    expect(result[0]).toEqual({
      role: "assistant",
      content: [
        {
          type: "tool-call",
          toolCallId: "call_1",
          toolName: "fetch",
          input: { url: "http://example.com" },
        },
      ],
    });
    expect(result[1]).toEqual({
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
    expect(result[2]).toEqual({
      role: "assistant",
      content: [
        {
          type: "tool-call",
          toolCallId: "call_2",
          toolName: "fetch",
          input: { url: "http://bad.local" },
        },
      ],
    });
    expect(result[3]).toEqual({
      role: "tool",
      content: [
        {
          type: "tool-result",
          toolCallId: "call_2",
          toolName: "fetch",
          output: { type: "error-text", value: "Network connection refused" },
        },
      ],
    });
  });

  it("3. Non-JSON lines and step_start events: quietly ignored without dropping stream", async () => {
    const stdout$ = new Subject<string>();
    const { provider } = setupProvider(stdout$);
    const emissionsPromise = firstValueFrom(
      provider.exec(createTestRunContext(), [{ role: "user", content: "hi" }]).pipe(toArray()),
    );
    await Promise.resolve();

    stdout$.next("Debug: Initializing OpenCode runtime...\n");
    stdout$.next(
      `${JSON.stringify({
        type: "step_start",
        timestamp: 100,
        sessionID: "ses_1",
        part: {
          id: "prt_1",
          messageID: "msg_1",
          sessionID: "ses_1",
          type: "step-start",
        },
      })}\n`,
    );
    stdout$.next(
      `${JSON.stringify({
        type: "text",
        timestamp: 101,
        sessionID: "ses_1",
        part: {
          id: "prt_2",
          messageID: "msg_1",
          sessionID: "ses_1",
          type: "text",
          text: "hello back",
        },
      })}\n`,
    );
    stdout$.next(
      `${JSON.stringify({
        type: "step_finish",
        timestamp: 102,
        sessionID: "ses_1",
        part: {
          id: "prt_3",
          messageID: "msg_1",
          sessionID: "ses_1",
          type: "step-finish",
          reason: "stop",
        },
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

  it("4. Failure result: throws error into stream for error event", async () => {
    const stdout$ = new Subject<string>();
    const { provider } = setupProvider(stdout$);
    const emissionsPromise = firstValueFrom(
      provider.exec(createTestRunContext(), [{ role: "user", content: "fail" }]).pipe(toArray()),
    );
    await Promise.resolve();

    stdout$.next(
      `${JSON.stringify({
        type: "error",
        timestamp: 100,
        sessionID: "ses_1",
        error: {
          name: "APIError",
          data: {
            message: "Fatal model error: invalid prompt",
          },
        },
      })}\n`,
    );
    stdout$.complete();

    expect(emissionsPromise).rejects.toThrow(
      "opencode execution failed: Fatal model error: invalid prompt",
    );
  });

  it("5. Unsubscribe: triggers local abort controller and cancels process", async () => {
    const stdout$ = new Subject<string>();
    const { provider, getSpawnOpts } = setupProvider(stdout$);
    const sub = provider
      .exec(createTestRunContext(), [{ role: "user", content: "long run" }])
      .subscribe();

    await Promise.resolve();

    const spawnOpts = getSpawnOpts();
    expect(spawnOpts?.signal).toBeDefined();
    expect(spawnOpts?.signal?.aborted).toBe(false);

    sub.unsubscribe();

    expect(spawnOpts?.signal?.aborted).toBe(true);
  });

  it("6. Text delta streaming: buffers preceding text into tool call and emits final clean assistant response", async () => {
    const stdout$ = new Subject<string>();
    const { provider } = setupProvider(stdout$);
    const ctx = createTestRunContext();
    const messages: ModelMessage[] = [{ role: "user", content: "Find documents" }];

    const emissionsPromise = firstValueFrom(provider.exec(ctx, messages).pipe(toArray()));
    await Promise.resolve();

    stdout$.next(
      `${JSON.stringify({
        type: "text",
        timestamp: 100,
        sessionID: "ses_1",
        part: {
          id: "prt_1",
          messageID: "msg_1",
          sessionID: "ses_1",
          type: "text",
          text: "Searching ",
        },
      })}\n`,
    );
    stdout$.next(
      `${JSON.stringify({
        type: "text",
        timestamp: 101,
        sessionID: "ses_1",
        part: {
          id: "prt_2",
          messageID: "msg_1",
          sessionID: "ses_1",
          type: "text",
          text: "in space...\n",
        },
      })}\n`,
    );

    stdout$.next(
      `${JSON.stringify({
        type: "tool_use",
        timestamp: 102,
        sessionID: "ses_1",
        part: {
          type: "tool",
          tool: "search",
          callID: "call_abc",
          state: {
            status: "completed",
            input: { query: "notes" },
            output: "found 1 note",
          },
        },
      })}\n`,
    );

    stdout$.next(
      `${JSON.stringify({
        type: "step_finish",
        timestamp: 103,
        sessionID: "ses_1",
        part: {
          id: "prt_3",
          messageID: "msg_1",
          sessionID: "ses_1",
          type: "step-finish",
          reason: "tool-calls",
        },
      })}\n`,
    );

    stdout$.next(
      `${JSON.stringify({
        type: "text",
        timestamp: 104,
        sessionID: "ses_1",
        part: {
          id: "prt_4",
          messageID: "msg_2",
          sessionID: "ses_1",
          type: "text",
          text: "Found 1 note. Here it is.",
        },
      })}\n`,
    );

    stdout$.next(
      `${JSON.stringify({
        type: "step_finish",
        timestamp: 105,
        sessionID: "ses_1",
        part: {
          id: "prt_5",
          messageID: "msg_2",
          sessionID: "ses_1",
          type: "step-finish",
          reason: "stop",
        },
      })}\n`,
    );
    stdout$.complete();

    const result = await emissionsPromise;
    expect(result).toHaveLength(3);

    expect(result[0]).toEqual({
      role: "assistant",
      content: [
        {
          type: "text",
          text: "Searching in space...",
        },
        {
          type: "tool-call",
          toolCallId: "call_abc",
          toolName: "search",
          input: { query: "notes" },
        },
      ],
    });

    expect(result[1]).toEqual({
      role: "tool",
      content: [
        {
          type: "tool-result",
          toolCallId: "call_abc",
          toolName: "search",
          output: { type: "text", value: "found 1 note" },
        },
      ],
    });

    expect(result[2]).toEqual({
      role: "assistant",
      content: "Found 1 note. Here it is.",
    });
  });

  it("7. Prompt serialization: serializes extended prompt with Tool Call Guidelines into stdin", async () => {
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
    expect(stdin).toContain("Tool Call Guidelines:");
    expect(stdin).toContain(
      "- Before calling ANY tool, output a short single-sentence explanation of what you are about to do.",
    );
    expect(stdin).toContain("[USER]:\nПривет");
  });
});
