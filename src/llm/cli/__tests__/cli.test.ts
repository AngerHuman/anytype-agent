import { describe, expect, it } from "bun:test";
import type { ModelMessage } from "ai";
import { firstValueFrom, Subject, toArray } from "rxjs";
import {
  createMockAliasHandler,
  createTestRunContext,
  mockApiParams,
  mockCliConfig,
} from "../../__tests__/test.fixtures";
import type { ApiAliasHandler } from "../index";
import { LlmCliProvider } from "../index";

class TestCliProvider extends LlmCliProvider {
  public readonly process = null;
  public capturedMessages: ModelMessage[] = [];
  public capturedSignal?: AbortSignal;
  public responseSubject = new Subject<ModelMessage>();

  protected execInternal(messages: ModelMessage[], abort?: AbortSignal) {
    this.capturedMessages = messages;
    this.capturedSignal = abort;
    return this.responseSubject.asObservable();
  }

  public getStoredHistory(key: string): ModelMessage[] | undefined {
    return this.history.get(key);
  }
}

describe("LlmCliProvider (Base Class Unit Tests)", () => {
  const createContext = (overrides = {}) =>
    createTestRunContext({
      spaceId: "space-base",
      chatId: "chat-base",
      triggerId: "msg-user-1",
      botId: "bot-base",
      ...overrides,
    });

  const setupProvider = (aliasHandler?: ApiAliasHandler) => {
    const handler = aliasHandler ?? createMockAliasHandler("alias-999");
    const provider = new TestCliProvider(mockCliConfig, handler, mockApiParams);
    return { provider, aliasHandler: handler };
  };

  it("1. System prompt & space alias lifecycle: issues alias, builds prompt and revokes on completion", async () => {
    const { provider, aliasHandler } = setupProvider();
    const ctx = createContext();
    const userMessage: ModelMessage = {
      role: "user",
      content: "Hi CLI",
      providerOptions: { message: { id: "msg-user-1" } },
    };

    const streamPromise = firstValueFrom(provider.exec(ctx, [userMessage]).pipe(toArray()));
    await Promise.resolve();

    expect(aliasHandler.issueSpaceAlias).toHaveBeenCalledWith("space-base");
    expect(provider.capturedMessages).toHaveLength(2);

    const systemMsg = provider.capturedMessages[0];
    expect(systemMsg?.role).toBe("system");
    expect(systemMsg?.content).toContain('You are the AI assistant "TestBot"');
    expect(systemMsg?.content).toContain(`http://localhost:${mockApiParams.port}`);
    expect(systemMsg?.content).toContain(mockApiParams.docs);
    expect(systemMsg?.content).toContain("Current space alias: alias-999");

    const passedUserMsg = provider.capturedMessages[1];
    expect(passedUserMsg).toEqual(userMessage);

    provider.responseSubject.next({ role: "assistant", content: "Hello!" });
    provider.responseSubject.complete();

    await streamPromise;

    expect(aliasHandler.revokeSpaceAlias).toHaveBeenCalledWith("alias-999");
  });

  it("2. Space alias lifecycle: revokes alias even when execInternal errors", async () => {
    const { provider, aliasHandler } = setupProvider();
    const ctx = createContext();

    const streamPromise = firstValueFrom(provider.exec(ctx, []).pipe(toArray()));
    await Promise.resolve();

    provider.responseSubject.error(new Error("Subprocess crashed"));

    expect(streamPromise).rejects.toThrow("Subprocess crashed");
    expect(aliasHandler.revokeSpaceAlias).toHaveBeenCalledWith("alias-999");
  });

  it("3. Tool history accumulation & enrichment across turns (omits assistant string answers)", async () => {
    const { provider } = setupProvider();

    // Turn 1: user triggers msg-1, LLM produces tool calls + final assistant text reply
    const ctx1 = createContext({ triggerId: "msg-1" });
    const userMsg1: ModelMessage = {
      role: "user",
      content: "Do search",
      providerOptions: { message: { id: "msg-1" } },
    };

    const stream1Promise = firstValueFrom(provider.exec(ctx1, [userMsg1]).pipe(toArray()));
    await Promise.resolve();

    const toolMsg: ModelMessage = {
      role: "tool",
      content: [
        {
          type: "tool-result",
          toolCallId: "c1",
          toolName: "search",
          output: { type: "text", value: "res" },
        },
      ],
    };
    const assistantMsg1: ModelMessage = {
      role: "assistant",
      content: "Here is your result",
    };

    provider.responseSubject.next(toolMsg);
    provider.responseSubject.next(assistantMsg1);
    provider.responseSubject.complete();

    await stream1Promise;

    // Verify turn 1 tools are saved in history under key (only toolMsg, assistant text omitted)
    const historyKey1 = "space-base-chat-base-bot-base-msg-1";
    expect(provider.getStoredHistory(historyKey1)).toEqual([toolMsg]);

    // Turn 2: next message in chat triggers msg-2
    provider.responseSubject = new Subject<ModelMessage>();
    const ctx2 = createContext({ triggerId: "msg-2" });
    const userMsg2: ModelMessage = {
      role: "user",
      content: "Thanks, another question",
      providerOptions: { message: { id: "msg-2" } },
    };

    const chatHistory = [userMsg1, assistantMsg1, userMsg2];

    const stream2Promise = firstValueFrom(provider.exec(ctx2, chatHistory).pipe(toArray()));
    await Promise.resolve();

    // Verify messages passed into execInternal:
    // [0]: system prompt
    // [1]: userMsg1
    // [2]: toolMsg (enriched from history)
    // [3]: assistantMsg1 (from original chat history)
    // [4]: userMsg2
    expect(provider.capturedMessages).toHaveLength(5);
    expect(provider.capturedMessages[0]?.role).toBe("system");
    expect(provider.capturedMessages[1]?.content).toBe("Do search");
    expect(provider.capturedMessages[2]).toEqual(toolMsg);
    expect(provider.capturedMessages[3]?.content).toBe("Here is your result");
    expect(provider.capturedMessages[4]?.content).toBe("Thanks, another question");

    provider.responseSubject.complete();
    await stream2Promise;
  });

  it("4. History resilience: does not save history when stream fails or when no tools were used", async () => {
    const { provider } = setupProvider();

    // Case A: Stream fails with error - history must not be saved
    const ctxError = createContext({ triggerId: "msg-err" });
    const streamErrPromise = firstValueFrom(provider.exec(ctxError, []).pipe(toArray()));
    await Promise.resolve();

    provider.responseSubject.next({
      role: "tool",
      content: [
        {
          type: "tool-result",
          toolCallId: "c_err",
          toolName: "fail",
          output: { type: "text", value: "x" },
        },
      ],
    });
    provider.responseSubject.error(new Error("LLM failure mid-execution"));
    expect(streamErrPromise).rejects.toThrow("LLM failure mid-execution");

    expect(provider.getStoredHistory("space-base-chat-base-bot-base-msg-err")).toBeUndefined();

    // Case B: Stream completes with NO tool calls - empty entry must not be saved
    provider.responseSubject = new Subject<ModelMessage>();
    const ctxNoTools = createContext({ triggerId: "msg-no-tools" });
    const streamNoToolsPromise = firstValueFrom(provider.exec(ctxNoTools, []).pipe(toArray()));
    await Promise.resolve();

    provider.responseSubject.next({ role: "assistant", content: "Direct answer without tools" });
    provider.responseSubject.complete();
    await streamNoToolsPromise;

    expect(provider.getStoredHistory("space-base-chat-base-bot-base-msg-no-tools")).toBeUndefined();
  });
});
