import { describe, expect, it } from "bun:test";
import { firstValueFrom, toArray } from "rxjs";
import type { ChatMessage } from "../../client";
import { LlmService } from "../llm.service";
import { LlmAction, LlmResponse, type LlmRunContext } from "../types";
import { createTestRunContext, MockLlmProvider } from "./test.fixtures";

describe("LlmService (Unit Tests)", () => {
  const createContext = (overrides: Partial<LlmRunContext> = {}) =>
    createTestRunContext({ botId: "bot123", ...overrides });

  it("1. Passes context and mapped messages with providerOptions to provider", async () => {
    const provider = new MockLlmProvider();
    const service = new LlmService([provider]);

    const ctx = createContext();
    const messages: ChatMessage[] = [
      {
        id: "msg-1",
        creator: "user_456",
        content: { text: "Hello bot!" },
      },
    ];

    const streamPromise = firstValueFrom(service.run(ctx, messages).pipe(toArray()));
    await Promise.resolve();

    expect(provider.capturedCtx).toBe(ctx);
    expect(provider.capturedMessages).toHaveLength(1);

    const userMsg = provider.capturedMessages[0];
    expect(userMsg?.role).toBe("user");
    expect(userMsg?.content).toBe("Hello bot!");
    expect(userMsg?.providerOptions).toEqual({ message: { id: "msg-1" } });

    // Emit assistant response
    provider.responseSubject.next({
      role: "assistant",
      content: "Hello human!",
    });
    provider.responseSubject.complete();

    const events = await streamPromise;
    expect(events).toHaveLength(1);
    expect(events[0]).toEqual(LlmResponse.create("Hello human!"));
  });

  it("2. Propagates errors from provider stream", async () => {
    const provider = new MockLlmProvider();
    const service = new LlmService([provider]);

    const ctx = createContext();
    const streamPromise = firstValueFrom(service.run(ctx, []).pipe(toArray()));
    await Promise.resolve();

    provider.responseSubject.error(new Error("Provider crash"));

    expect(streamPromise).rejects.toThrow("Provider crash");
  });

  it("3. Mapping & Streaming: maps tool messages to LlmAction and string content to LlmResponse", async () => {
    const provider = new MockLlmProvider();
    const service = new LlmService([provider]);

    const ctx = createContext();
    const streamPromise = firstValueFrom(service.run(ctx, []).pipe(toArray()));
    await Promise.resolve();

    provider.responseSubject.next({
      role: "tool",
      content: [
        {
          type: "tool-result",
          toolCallId: "call_1",
          toolName: "search",
          output: { type: "text", value: "search results" },
        },
      ],
    });
    provider.responseSubject.next({
      role: "assistant",
      content: "Here is your answer.",
    });
    provider.responseSubject.complete();

    const events = await streamPromise;
    expect(events).toHaveLength(2);
    expect(events[0]).toBeInstanceOf(LlmAction);
    expect((events[0] as LlmAction).detail).toContain("search results");
    expect(events[1]).toBeInstanceOf(LlmResponse);
    expect((events[1] as LlmResponse).text).toBe("Here is your answer.");
  });

  it("4. Bot creator detection: correctly identifies bot messages by suffix", async () => {
    const provider = new MockLlmProvider();
    const service = new LlmService([provider]);

    const ctx = createContext({ botId: "mybot_id" });
    const messages: ChatMessage[] = [
      {
        id: "1",
        creator: "user_123",
        content: { text: "hi" },
      },
      {
        id: "2",
        creator: "prefix_mybot_id",
        content: { text: "hello there" },
      },
      {
        id: "3",
        creator: "other_bot_notmine",
        content: { text: "im someone else" },
      },
    ];

    const streamPromise = firstValueFrom(service.run(ctx, messages).pipe(toArray()));
    await Promise.resolve();

    expect(provider.capturedMessages[0]?.role).toBe("user");
    expect(provider.capturedMessages[0]?.providerOptions).toEqual({ message: { id: "1" } });

    expect(provider.capturedMessages[1]?.role).toBe("assistant");
    expect(provider.capturedMessages[1]?.providerOptions).toEqual({ message: { id: "2" } });

    expect(provider.capturedMessages[2]?.role).toBe("user");
    expect(provider.capturedMessages[2]?.providerOptions).toEqual({ message: { id: "3" } });

    provider.responseSubject.complete();
    await streamPromise;
  });

  it("5. Pre-aborted context returns EMPTY immediately", async () => {
    const provider = new MockLlmProvider();
    const service = new LlmService([provider]);

    const abortController = new AbortController();
    abortController.abort();

    const ctx = createContext({ abort: abortController.signal });
    const events = await firstValueFrom(service.run(ctx, []).pipe(toArray()));

    expect(events).toHaveLength(0);
    expect(provider.capturedCtx).toBeUndefined();
  });

  it("6. Assistant tool-call with accompanying TextPart in content emits LlmAction", async () => {
    const provider = new MockLlmProvider();
    const service = new LlmService([provider]);
    const ctx = createContext();

    const streamPromise = firstValueFrom(service.run(ctx, []).pipe(toArray()));
    await Promise.resolve();

    provider.responseSubject.next({
      role: "assistant",
      content: [
        {
          type: "text",
          text: "Поиск документов",
        },
        {
          type: "tool-call",
          toolCallId: "call_1",
          toolName: "search",
          input: {},
        },
      ],
    });

    provider.responseSubject.next({
      role: "assistant",
      content: "Готово",
    });

    provider.responseSubject.complete();
    const events = await streamPromise;

    expect(events).toHaveLength(2);
    expect(events[0]).toBeInstanceOf(LlmAction);
    expect((events[0] as LlmAction).detail).toBe("Поиск документов");

    expect(events[1]).toBeInstanceOf(LlmResponse);
    expect((events[1] as LlmResponse).text).toBe("Готово");
  });
});
