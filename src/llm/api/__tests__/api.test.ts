import { describe, expect, it, mock } from "bun:test";
import type { MCPClient } from "@ai-sdk/mcp";
import type { LanguageModelV4Prompt, LanguageModelV4StreamPart } from "@ai-sdk/provider";
import type { ModelMessage, ToolSet } from "ai";
import { simulateReadableStream, tool } from "ai";
import { MockLanguageModelV4 } from "ai/test";
import { firstValueFrom, toArray } from "rxjs";
import { z } from "zod";
import type { AppConfig, LlmApi } from "../../../config/config.schema";
import { createTestRunContext } from "../../__tests__/test.fixtures";
import { LlmApiProvider } from "../index";

const simulateStream = (chunks: LanguageModelV4StreamPart[]) =>
  simulateReadableStream<LanguageModelV4StreamPart>({ chunks });

const finishChunk = (unified: "stop" | "tool-calls" = "stop"): LanguageModelV4StreamPart => ({
  type: "finish",
  finishReason: { unified, raw: unified },
  usage: {
    inputTokens: { total: 0, noCache: undefined, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 0, text: undefined, reasoning: undefined },
  },
});

describe("LlmApiProvider (Stream & History Unit Tests)", () => {
  const mockConfig: AppConfig = {
    ANYTYPE: {
      API_URL: "http://127.0.0.1:31012",
      API_KEY: "test_key",
      BOT_NAME: "TestBot",
      API_VERSION: "2025-11-08",
    },
    SETTINGS: {
      DEBOUNCE_MS: 800,
      RETRY_DELAY_MS: 3000,
      SCAN_INTERVAL_MS: 60000,
      MAX_ACTIVE_CHATS: 50,
    },
    LLM: [],
    PROXY_PORT: 31013,
  };

  const mockApiConfig: LlmApi = {
    NAME: "test-api",
    MODE: "api",
    API_URL: "https://api.test.ai/v1",
    API_KEY: "secret",
    MODEL: "test-model",
  };

  const createMockMcp = (tools: ToolSet = {}) =>
    ({
      tools: mock(async () => tools),
    }) as unknown as MCPClient;

  it("1. Lazy tool initialization: caches tools after first call", async () => {
    const mockMcp = createMockMcp();
    const model = new MockLanguageModelV4({
      doStream: async () => ({
        stream: simulateStream([finishChunk()]),
      }),
    });
    const provider = new LlmApiProvider(mockMcp, mockConfig, mockApiConfig, model);

    const ctx = createTestRunContext();
    const sub1 = provider.exec(ctx, [{ role: "user", content: "hi" }]).subscribe();
    const sub2 = provider.exec(ctx, [{ role: "user", content: "hi" }]).subscribe();

    await Promise.resolve();

    expect(mockMcp.tools).toHaveBeenCalledTimes(1);

    sub1.unsubscribe();
    sub2.unsubscribe();
  });

  it("2. Tool events mapping: maps tool-call and tool-result correctly", async () => {
    const mockMcp = createMockMcp({
      "API-get-space": tool({
        description: "Get space",
        inputSchema: z.object({ space_id: z.string() }),
      }),
    });

    const model = new MockLanguageModelV4({
      doStream: async () => ({
        stream: simulateStream([
          {
            type: "tool-call",
            toolCallId: "call_1",
            toolName: "API-get-space",
            input: JSON.stringify({ space_id: "sp_1" }),
          },
          {
            type: "tool-result",
            toolCallId: "call_1",
            toolName: "API-get-space",
            result: { name: "My Space" },
          },
          finishChunk("stop"),
        ]),
      }),
    });
    const provider = new LlmApiProvider(mockMcp, mockConfig, mockApiConfig, model);

    const ctx = createTestRunContext();
    const messages = await firstValueFrom(
      provider.exec(ctx, [{ role: "user", content: "get space" }]).pipe(toArray()),
    );

    expect(messages).toHaveLength(2);

    expect(messages[0]).toEqual({
      role: "assistant",
      content: [
        {
          type: "tool-call",
          toolCallId: "call_1",
          toolName: "API-get-space",
          input: { space_id: "sp_1" },
        },
      ],
    });

    expect(messages[1]).toEqual({
      role: "tool",
      content: [
        {
          type: "tool-result",
          toolCallId: "call_1",
          toolName: "API-get-space",
          output: {
            type: "json",
            value: { name: "My Space" },
          },
        },
      ],
    });
  });

  it("3. Text streaming: accumulates text deltas and emits final message on finish", async () => {
    const model = new MockLanguageModelV4({
      doStream: async () => ({
        stream: simulateStream([
          { type: "text-start", id: "t1" },
          { type: "text-delta", id: "t1", delta: "Hello, " },
          { type: "text-delta", id: "t1", delta: "world!" },
          { type: "text-end", id: "t1" },
          finishChunk("stop"),
        ]),
      }),
    });
    const provider = new LlmApiProvider(createMockMcp(), mockConfig, mockApiConfig, model);

    const messages = await firstValueFrom(
      provider
        .exec(createTestRunContext(), [{ role: "user", content: "say hello" }])
        .pipe(toArray()),
    );

    expect(messages).toEqual([
      {
        role: "assistant",
        content: "Hello, world!",
      },
    ]);
  });

  it("4. Tool History: accumulates tool events and enriches subsequent calls", async () => {
    let capturedPrompt: LanguageModelV4Prompt | undefined;

    const mockMcp = createMockMcp({
      "API-create-task": tool({
        description: "Create task",
        inputSchema: z.object({ title: z.string() }),
      }),
    });

    let callCount = 0;
    const model = new MockLanguageModelV4({
      doStream: async (options) => {
        callCount++;
        if (callCount === 1) {
          return {
            stream: simulateStream([
              {
                type: "tool-call",
                toolCallId: "call_99",
                toolName: "API-create-task",
                input: JSON.stringify({ title: "Test" }),
              },
              {
                type: "tool-result",
                toolCallId: "call_99",
                toolName: "API-create-task",
                result: { id: "task_1" },
              },
              finishChunk("stop"),
            ]),
          };
        }

        capturedPrompt = options.prompt;
        return {
          stream: simulateStream([finishChunk("stop")]),
        };
      },
    });

    const provider = new LlmApiProvider(mockMcp, mockConfig, mockApiConfig, model);

    const ctx = createTestRunContext({ triggerId: "trigger_123" });
    await firstValueFrom(
      provider.exec(ctx, [{ role: "user", content: "create task" }]).pipe(toArray()),
    );

    const history: ModelMessage[] = [
      {
        role: "user",
        content: "create task",
        providerOptions: { message: { id: "trigger_123" } },
      },
      {
        role: "assistant",
        content: "Task created.",
      },
      {
        role: "user",
        content: "What did you do?",
        providerOptions: { message: { id: "trigger_456" } },
      },
    ];

    const nextCtx = createTestRunContext({ triggerId: "trigger_456" });
    await firstValueFrom(provider.exec(nextCtx, history).pipe(toArray()));

    expect(capturedPrompt).toBeDefined();
    const roles = capturedPrompt?.map((p) => p.role);
    expect(roles).toEqual(["system", "user", "assistant", "tool", "assistant", "user"]);
  });

  it("5. History resilience: does not save history if stream errors", async () => {
    let capturedPrompt: LanguageModelV4Prompt | undefined;
    let callCount = 0;

    const model = new MockLanguageModelV4({
      doStream: async (options) => {
        callCount++;
        if (callCount === 1) {
          return {
            stream: simulateStream([
              {
                type: "error",
                error: new Error("Network fail"),
              },
            ]),
          };
        }

        capturedPrompt = options.prompt;
        return {
          stream: simulateStream([finishChunk("stop")]),
        };
      },
    });

    const provider = new LlmApiProvider(createMockMcp(), mockConfig, mockApiConfig, model);

    const ctx = createTestRunContext({ triggerId: "trigger_err" });
    expect(
      firstValueFrom(
        provider.exec(ctx, [{ role: "user", content: "trigger error" }]).pipe(toArray()),
      ),
    ).rejects.toThrow("Network fail");

    const userMessage: ModelMessage = {
      role: "user",
      content: "Repeat",
      providerOptions: { message: { id: "trigger_err" } },
    };

    await firstValueFrom(provider.exec(createTestRunContext(), [userMessage]).pipe(toArray()));

    expect(capturedPrompt).toBeDefined();
    const roles = capturedPrompt?.map((p) => p.role);
    expect(roles).toEqual(["system", "user"]);
  });

  it("6. Session header and system prompt: passes x-opencode-session and bot name", async () => {
    let capturedHeaders: Record<string, string | undefined> | undefined;
    let capturedSystem: string | undefined;

    const model = new MockLanguageModelV4({
      doStream: async (options) => {
        capturedHeaders = options.headers;
        const systemItem = options.prompt.find((p) => p.role === "system");
        capturedSystem =
          systemItem && "content" in systemItem && typeof systemItem.content === "string"
            ? systemItem.content
            : undefined;

        return {
          stream: simulateStream([finishChunk("stop")]),
        };
      },
    });

    const provider = new LlmApiProvider(createMockMcp(), mockConfig, mockApiConfig, model);
    const ctx = createTestRunContext();
    await firstValueFrom(
      provider.exec(ctx, [{ role: "user", content: "test headers" }]).pipe(toArray()),
    );

    expect(capturedHeaders).toEqual({
      "x-opencode-session": `${ctx.spaceId}:${ctx.chatId}`,
    });
    expect(capturedSystem).toContain('You are the AI assistant "TestBot" in Anytype.');
  });
});
