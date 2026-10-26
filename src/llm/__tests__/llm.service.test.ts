import { describe, expect, it } from "bun:test";
import { firstValueFrom, toArray } from "rxjs";
import type { ChatMessage } from "../../client/schema";
import { LlmService } from "../llm.service";
import { LlmResponse, type LlmRunContext } from "../types";
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
    expect(events[0]).toEqual({
      type: "response",
      data: LlmResponse.create("Hello human!"),
    });
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

  it("3. Mapping & Streaming: maps tool messages to LlmAction[] and string content to LlmResponse", async () => {
    const provider = new MockLlmProvider();
    const service = new LlmService([provider]);

    const ctx = createContext();
    const streamPromise = firstValueFrom(service.run(ctx, []).pipe(toArray()));
    await Promise.resolve();

    provider.responseSubject.next({
      role: "assistant",
      content: [
        {
          type: "tool-call",
          toolCallId: "call_1",
          toolName: "search",
          input: { query: "test" },
        },
      ],
    });

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
    expect(events).toHaveLength(3);

    // 1st event: actions with pending status
    const firstEvent = events[0];
    expect(firstEvent?.type).toBe("actions");
    if (firstEvent?.type === "actions") {
      expect(firstEvent.data[0]?.comment).toBe("search");
      expect(firstEvent.data[0]?.status).toBe("pending");
    }

    // 2nd event: actions with done status
    const secondEvent = events[1];
    expect(secondEvent?.type).toBe("actions");
    if (secondEvent?.type === "actions") {
      expect(secondEvent.data[0]?.status).toBe("done");
    }

    // 3rd event: final response
    const thirdEvent = events[2];
    expect(thirdEvent?.type).toBe("response");
    if (thirdEvent?.type === "response") {
      expect(thirdEvent.data.text).toBe("Here is your answer.");
    }
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

  it("6. Assistant tool-call with accompanying TextPart in content emits LlmAction[]", async () => {
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
    expect(events[0]?.type).toBe("actions");
    if (events[0]?.type === "actions") {
      expect(events[0].data[0]?.comment).toBe("Поиск документов");
    }

    expect(events[1]?.type).toBe("response");
    if (events[1]?.type === "response") {
      expect(events[1].data.text).toBe("Готово");
    }
  });

  it("7. Assistant message with content array containing only text emits response", async () => {
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
          text: "Ты уверен?",
        },
      ],
    });

    provider.responseSubject.complete();
    const events = await streamPromise;

    expect(events).toHaveLength(1);
    expect(events[0]?.type).toBe("response");
    if (events[0]?.type === "response") {
      expect(events[0].data.text).toBe("Ты уверен?");
    }
  });

  it("8. Action status remains pending when one tool fails while others are pending, then turns error", async () => {
    const provider = new MockLlmProvider();
    const service = new LlmService([provider]);
    const ctx = createContext();

    const streamPromise = firstValueFrom(service.run(ctx, []).pipe(toArray()));
    await Promise.resolve();

    provider.responseSubject.next({
      role: "assistant",
      content: [
        {
          type: "tool-call",
          toolCallId: "call_1",
          toolName: "curl",
          input: {},
        },
        {
          type: "tool-call",
          toolCallId: "call_2",
          toolName: "python",
          input: {},
        },
      ],
    });

    // 1st tool fails
    provider.responseSubject.next({
      role: "tool",
      content: [
        {
          type: "tool-result",
          toolCallId: "call_1",
          toolName: "curl",
          output: { type: "error-text", value: "Connection refused" },
        },
      ],
    });

    // 2nd tool succeeds
    provider.responseSubject.next({
      role: "tool",
      content: [
        {
          type: "tool-result",
          toolCallId: "call_2",
          toolName: "python",
          output: { type: "text", value: "ok" },
        },
      ],
    });

    provider.responseSubject.complete();
    const events = await streamPromise;

    expect(events).toHaveLength(3);

    // Initial state: 0/2 done -> pending
    const ev1 = events[0];
    expect(ev1?.type).toBe("actions");
    if (ev1?.type === "actions") {
      expect(ev1.data[0]?.status).toBe("pending");
    }

    // After 1st failure: 1/2 done, 1 pending -> STILL pending (not sticky premature error!)
    const ev2 = events[1];
    expect(ev2?.type).toBe("actions");
    if (ev2?.type === "actions") {
      expect(ev2.data[0]?.status).toBe("pending");
      expect(ev2.data[0]?.error).toBe("Connection refused");
    }

    // After 2nd tool completes: 2/2 done, has error -> NOW error
    const ev3 = events[2];
    expect(ev3?.type).toBe("actions");
    if (ev3?.type === "actions") {
      expect(ev3.data[0]?.status).toBe("error");
    }
  });
});
