import { describe, expect, it, mock, spyOn } from "bun:test";
import type { MCPClient } from "@ai-sdk/mcp";
import type { ModelMessage, TextStreamPart } from "ai";
import * as aiModule from "ai";
import { firstValueFrom, Subject, toArray } from "rxjs";
import type { AppConfig, LlmApi } from "../../../config/config.schema";
import { createTestRunContext } from "../../__tests__/test.fixtures";
import { LlmApiProvider } from "../index";

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

  const createMockMcp = (tools = { dummy_tool: {} }) =>
    ({
      tools: mock(async () => tools),
    }) as unknown as MCPClient;

  it("1. Lazy tool initialization: caches tools after first call", async () => {
    const mockMcp = createMockMcp();
    const provider = new LlmApiProvider(mockMcp, mockConfig, mockApiConfig);

    const stream$ = new Subject<TextStreamPart<any>>();
    const streamTextSpy = spyOn(aiModule, "streamText").mockReturnValue({
      stream: stream$ as any,
    } as any);

    const ctx = createTestRunContext();
    const sub1 = provider.exec(ctx, []).subscribe();
    const sub2 = provider.exec(ctx, []).subscribe();

    // Ждем микротаску резолва getTools()
    await Promise.resolve();

    expect(mockMcp.tools).toHaveBeenCalledTimes(1);

    sub1.unsubscribe();
    sub2.unsubscribe();
    streamTextSpy.mockRestore();
  });

  it("2. Tool events mapping: maps tool-call and tool-result correctly", async () => {
    const mockMcp = createMockMcp();
    const provider = new LlmApiProvider(mockMcp, mockConfig, mockApiConfig);

    const stream$ = new Subject<TextStreamPart<any>>();
    const streamTextSpy = spyOn(aiModule, "streamText").mockReturnValue({
      stream: stream$ as any,
    } as any);

    const ctx = createTestRunContext();
    const messagesPromise = firstValueFrom(provider.exec(ctx, []).pipe(toArray()));

    // Даем switchMap подписаться на stream$
    await Promise.resolve();

    // Эмитим вызов тула
    stream$.next({
      type: "tool-call",
      toolCallId: "call_1",
      toolName: "API-get-space",
      input: { space_id: "sp_1" },
    });

    // Эмитим результат тула
    stream$.next({
      type: "tool-result",
      toolCallId: "call_1",
      toolName: "API-get-space",
      output: { name: "My Space" },
    });

    stream$.complete();

    const messages = await messagesPromise;
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

    streamTextSpy.mockRestore();
  });

  it("3. Text streaming: accumulates text deltas and emits final message on finish", async () => {
    const mockMcp = createMockMcp();
    const provider = new LlmApiProvider(mockMcp, mockConfig, mockApiConfig);

    const stream$ = new Subject<TextStreamPart<any>>();
    const streamTextSpy = spyOn(aiModule, "streamText").mockReturnValue({
      stream: stream$ as any,
    } as any);

    const ctx = createTestRunContext();
    const messagesPromise = firstValueFrom(provider.exec(ctx, []).pipe(toArray()));

    await Promise.resolve();

    stream$.next({ type: "text-delta", text: "Привет, " });
    stream$.next({ type: "text-delta", text: "мир!" });
    stream$.next({ type: "finish", finishReason: "stop" } as any);
    stream$.complete();

    const messages = await messagesPromise;
    expect(messages).toEqual([
      {
        role: "assistant",
        content: "Привет, мир!",
      },
    ]);

    streamTextSpy.mockRestore();
  });

  it("4. Tool History: accumulates tool events and enriches subsequent calls", async () => {
    const mockMcp = createMockMcp();
    const provider = new LlmApiProvider(mockMcp, mockConfig, mockApiConfig);

    const stream1$ = new Subject<TextStreamPart<any>>();
    let streamTextCallArgs: any;
    const streamTextSpy = spyOn(aiModule, "streamText").mockImplementation((args: any) => {
      streamTextCallArgs = args;
      return { stream: stream1$ as any } as any;
    });

    const ctx = createTestRunContext({ triggerId: "trigger_123" });
    const firstExecPromise = firstValueFrom(provider.exec(ctx, []).pipe(toArray()));

    await Promise.resolve();

    // Ход 1: вызываем тул и финишируем
    stream1$.next({
      type: "tool-call",
      toolCallId: "call_99",
      toolName: "API-create-task",
      input: { title: "Test" },
    });
    stream1$.next({
      type: "tool-result",
      toolCallId: "call_99",
      toolName: "API-create-task",
      output: { id: "task_1" },
    });
    stream1$.next({ type: "finish", finishReason: "stop" } as any);
    stream1$.complete();

    await firstExecPromise;

    // Ход 2: юзер шлет новое сообщение, ссылаясь на предыдущий триггер через providerOptions
    const stream2$ = new Subject<TextStreamPart<any>>();
    streamTextSpy.mockImplementation((args: any) => {
      streamTextCallArgs = args;
      return { stream: stream2$ as any } as any;
    });

    const userMessage: ModelMessage = {
      role: "user",
      content: "А что ты сделал?",
      providerOptions: { message: { id: "trigger_123" } },
    };

    const nextCtx = createTestRunContext({ triggerId: "trigger_456" });
    const sub = provider.exec(nextCtx, [userMessage]).subscribe();

    await Promise.resolve();

    // Проверяем, что в streamText улетели обогащенные сообщения с tool_call и tool_result из истории!
    expect(streamTextCallArgs.messages).toHaveLength(3);
    expect(streamTextCallArgs.messages[0]).toEqual(userMessage);
    expect(streamTextCallArgs.messages[1].role).toBe("assistant");
    expect(streamTextCallArgs.messages[2].role).toBe("tool");

    sub.unsubscribe();
    streamTextSpy.mockRestore();
  });

  it("5. History resilience: does not save history if stream errors", async () => {
    const mockMcp = createMockMcp();
    const provider = new LlmApiProvider(mockMcp, mockConfig, mockApiConfig);

    const stream$ = new Subject<TextStreamPart<any>>();
    const streamTextSpy = spyOn(aiModule, "streamText").mockReturnValue({
      stream: stream$ as any,
    } as any);

    const ctx = createTestRunContext({ triggerId: "trigger_err" });
    const execPromise = firstValueFrom(provider.exec(ctx, []).pipe(toArray()));

    await Promise.resolve();

    stream$.next({
      type: "tool-call",
      toolCallId: "call_err",
      toolName: "API-fail",
      input: {},
    });
    stream$.error(new Error("Network fail"));

    await expect(execPromise).rejects.toThrow("Network fail");

    // Проверяем следующее обращение: история должна быть пустой
    let secondCallArgs: any;
    streamTextSpy.mockImplementation((args: any) => {
      secondCallArgs = args;
      return { stream: new Subject() as any } as any;
    });

    const userMessage: ModelMessage = {
      role: "user",
      content: "Повтори",
      providerOptions: { message: { id: "trigger_err" } },
    };

    const sub = provider.exec(createTestRunContext(), [userMessage]).subscribe();
    await Promise.resolve();

    expect(secondCallArgs.messages).toEqual([userMessage]);

    sub.unsubscribe();
    streamTextSpy.mockRestore();
  });

  it("6. Session header and system prompt: passes x-opencode-session and bot name", async () => {
    const mockMcp = createMockMcp();
    const provider = new LlmApiProvider(mockMcp, mockConfig, mockApiConfig);

    let capturedArgs: any;
    const streamTextSpy = spyOn(aiModule, "streamText").mockImplementation((args: any) => {
      capturedArgs = args;
      return { stream: new Subject() as any } as any;
    });

    const ctx = createTestRunContext();
    const sub = provider.exec(ctx, []).subscribe();

    await Promise.resolve();

    expect(capturedArgs.headers).toEqual({
      "x-opencode-session": `${ctx.spaceId}:${ctx.chatId}`,
    });
    expect(capturedArgs.system).toContain('You are the AI assistant "TestBot" in Anytype.');

    sub.unsubscribe();
    streamTextSpy.mockRestore();
  });
});
