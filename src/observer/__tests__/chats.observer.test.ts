import "reflect-metadata";
import { describe, expect, it, mock } from "bun:test";
import { defer, Observable, of, Subject } from "rxjs";
import type { AnytypeService } from "../../client/anytype.service";
import type { Chat, ChatMessage } from "../../client/schema";
import type { ChatEvent } from "../../client/types";
import { makeAppConfig } from "../../config/__tests__/fixtures";
import type { LlmService } from "../../llm/llm.service";
import { LlmAction, type LlmEvent, LlmResponse, type LlmRunContext } from "../../llm/types";
import { ChatsObserver, ChatsObserverFactory } from "../chats.observer";
import { callArg, callCount, makeMessage, sleep, waitFor } from "./helpers";

describe("ChatsObserver (Unit Tests)", () => {
  const setup = (
    options: {
      getChats?: () => Promise<Chat[]>;
      getChatMessages?: (spaceId: string, chatId: string) => Promise<ChatMessage[]>;
      getChatMessage?: (spaceId: string, chatId: string, messageId: string) => Promise<ChatMessage>;
      llmHandler?: (ctx: LlmRunContext, history: ChatMessage[]) => Observable<LlmEvent>;
      scanIntervalMs?: number;
      editChatMessage?: (
        spaceId: string,
        chatId: string,
        messageId: string,
        body: unknown,
      ) => Promise<unknown>;
      deleteChatMessage?: (spaceId: string, chatId: string, messageId: string) => Promise<unknown>;
    } = {},
  ) => {
    const sseSubjects = new Map<string, Subject<ChatEvent>>();

    const anytypeFake = {
      getChats: mock(
        options.getChats ??
          (async () =>
            [
              { id: "chat.1", name: "Chat 1" },
              { id: "chat.2", name: "Chat 2" },
            ] as Chat[]),
      ),
      getChatMessages: mock(options.getChatMessages ?? (async () => [] as ChatMessage[])),
      getChatMessage: mock(
        options.getChatMessage ??
          (async (_spaceId: string, _chatId: string, messageId: string) =>
            makeMessage({
              id: messageId,
              creator: "_participant_space.1_member_test_bot",
            }) as unknown as ChatMessage),
      ),
      addChatMessage: mock(async (_spaceId: string, _chatId: string, _body: unknown) => ({
        message_id: `prog_${Date.now()}_${Math.random()}`,
      })),
      editChatMessage: mock(options.editChatMessage ?? (async () => ({}))),
      deleteChatMessage: mock(options.deleteChatMessage ?? (async () => ({}))),
      subscribeChatMessages: mock((_spaceId: string, chatId: string): Observable<ChatEvent> => {
        return defer(() => {
          let sub = sseSubjects.get(chatId);
          if (!sub) {
            sub = new Subject<ChatEvent>();
            sseSubjects.set(chatId, sub);
          }
          return sub;
        });
      }),
    } as unknown as AnytypeService;

    const llmFake = {
      run: mock(
        options.llmHandler ??
          (() => of<LlmEvent>({ type: "response", data: LlmResponse.create("  Bot reply  ") })),
      ),
    } as unknown as LlmService;

    const configMock = makeAppConfig({
      SETTINGS: {
        SCAN_INTERVAL_MS: options.scanIntervalMs ?? 60_000,
        RETRY_DELAY_MS: 10,
        DEBOUNCE_MS: 10,
      },
    });

    const observer = new ChatsObserver(
      "space.1",
      "member_test_bot",
      anytypeFake,
      llmFake,
      configMock,
    );

    const nextEvents: unknown[] = [];
    const errors: unknown[] = [];
    let completed = false;

    const subscription = observer.run().subscribe({
      next: (v) => nextEvents.push(v),
      error: (err) => errors.push(err),
      complete: () => {
        completed = true;
      },
    });

    const pushEvent = (chatId: string, event: ChatEvent) => {
      let sub = sseSubjects.get(chatId);
      if (!sub) {
        sub = new Subject<ChatEvent>();
        sseSubjects.set(chatId, sub);
      }
      sub.next(event);
    };

    const ready = async () => {
      await waitFor(() => sseSubjects.size >= 1);
    };

    return {
      observer,
      anytypeFake,
      llmFake,
      subscription,
      nextEvents,
      errors,
      getCompleted: () => completed,
      pushEvent,
      getSseSubject: (chatId: string) => sseSubjects.get(chatId),
      ready,
    };
  };

  it("1. Discovery: subscribes to SSE for all discovered chats", async () => {
    const { ready, anytypeFake } = setup();

    await ready();

    expect(callCount(anytypeFake.getChats)).toBe(1);
    expect(callCount(anytypeFake.subscribeChatMessages)).toBe(2);
  });

  it("2. Anti-echo: message_added from bot (creator suffix) -> LLM is not called", async () => {
    const { ready, pushEvent, llmFake } = setup();

    await ready();

    pushEvent("chat.1", {
      ...makeMessage({ creator: "_participant_space.1_member_test_bot", creator_name: "TestBot" }),
      type: "message_added",
    });

    await sleep(40);
    expect(callCount(llmFake.run)).toBe(0);
  });

  it("3. Direct Mention: message_added mentioning bot -> creates progress, runs LLM, posts reply, deletes progress", async () => {
    const { ready, pushEvent, anytypeFake, llmFake } = setup();

    await ready();

    pushEvent("chat.1", {
      ...makeMessage({ id: "msg_user", creator: "user_42" }, { mentionBot: "member_test_bot" }),
      type: "message_added",
    });

    await waitFor(() => callCount(llmFake.run) === 1);

    expect(callCount(anytypeFake.addChatMessage)).toBe(2); // 1: Progress, 2: Final response
    expect(callCount(anytypeFake.getChatMessages)).toBe(1); // Fetch history
    expect(callCount(anytypeFake.deleteChatMessage)).toBe(1); // Teardown progress

    const progressPost = callArg<{ text: string; reply_to_message_id?: string }>(
      anytypeFake.addChatMessage,
      0,
      2,
    );
    expect(progressPost.reply_to_message_id).toBe("msg_user");

    const finalPost = callArg<{ text: string; reply_to_message_id?: string }>(
      anytypeFake.addChatMessage,
      1,
      2,
    );
    expect(finalPost.text).toBe("Bot reply");
    expect(finalPost.reply_to_message_id).toBe("msg_user");
  });

  it("4. Reply to Bot: message replying to bot -> checks parent with getChatMessage and triggers LLM", async () => {
    const { ready, pushEvent, anytypeFake, llmFake } = setup();

    await ready();

    pushEvent("chat.1", {
      ...makeMessage({
        id: "msg_reply",
        creator: "user_42",
        reply_to_message_id: "msg_parent",
      }),
      type: "message_added",
    });

    await waitFor(() => callCount(llmFake.run) === 1);

    expect(callCount(anytypeFake.getChatMessage)).toBe(1);
    expect(callCount(llmFake.run)).toBe(1);
  });

  it("5. Reply to other user: message replying to non-bot -> getChatMessage returns other user -> LLM is NOT called", async () => {
    const { ready, pushEvent, anytypeFake, llmFake } = setup({
      getChatMessage: async () =>
        makeMessage({
          id: "msg_parent",
          creator: "_participant_space.1_user_other",
        }) as unknown as ChatMessage,
    });

    await ready();

    pushEvent("chat.1", {
      ...makeMessage({
        id: "msg_reply",
        creator: "user_42",
        reply_to_message_id: "msg_parent",
      }),
      type: "message_added",
    });

    await sleep(50);

    expect(callCount(anytypeFake.getChatMessage)).toBe(1);
    expect(callCount(llmFake.run)).toBe(0);
  });

  it("6. Preemption: new mention in same chat cancels in-flight LLM via abort signal", async () => {
    let abortSignal1: AbortSignal | undefined;
    let abortSignal2: AbortSignal | undefined;

    const { ready, pushEvent, llmFake } = setup({
      llmHandler: (ctx) => {
        if (!abortSignal1) {
          abortSignal1 = ctx.abort;
          return new Observable<LlmEvent>((subscriber) => {
            const timer = setTimeout(() => {
              subscriber.next(LlmResponse.create("First reply"));
              subscriber.complete();
            }, 500);
            return () => clearTimeout(timer);
          });
        }
        abortSignal2 = ctx.abort;
        return of(LlmResponse.create("Second reply"));
      },
    });

    await ready();

    // First trigger
    pushEvent("chat.1", {
      ...makeMessage({ id: "msg_1" }, { mentionBot: "member_test_bot" }),
      type: "message_added",
    });

    await waitFor(() => Boolean(abortSignal1));
    expect(abortSignal1?.aborted).toBe(false);

    // Second trigger in same chat (preempts first)
    pushEvent("chat.1", {
      ...makeMessage({ id: "msg_2" }, { mentionBot: "member_test_bot" }),
      type: "message_added",
    });

    await waitFor(() => Boolean(abortSignal2));

    expect(abortSignal1?.aborted).toBe(true);
    expect(callCount(llmFake.run)).toBe(2);
  });

  it("7. LlmAction Streaming: intermediate actions edit progress message with italic marks", async () => {
    const { ready, pushEvent, anytypeFake } = setup({
      llmHandler: () =>
        of<LlmEvent>(
          { type: "actions", data: [LlmAction.create("Tool 1 called")] },
          {
            type: "actions",
            data: [LlmAction.create("Tool 1 called"), LlmAction.create("Tool 2 called")],
          },
          { type: "response", data: LlmResponse.create("Final output") },
        ),
    });

    await ready();

    pushEvent("chat.1", {
      ...makeMessage({ id: "msg_stream" }, { mentionBot: "member_test_bot" }),
      type: "message_added",
    });

    await waitFor(() => callCount(anytypeFake.editChatMessage) === 2);

    expect(callCount(anytypeFake.editChatMessage)).toBe(2);
    const firstEdit = callArg<{ text: string; marks: { type: string }[] }>(
      anytypeFake.editChatMessage,
      0,
      3,
    );
    expect(firstEdit.text).toContain("Tool 1 called");
    expect(firstEdit.marks[0]?.type).toBe("italic");
  });

  it("8. ChatsObserverFactory: creates instance implementing ObserverFactory", () => {
    const factory = new ChatsObserverFactory(
      {} as AnytypeService,
      {} as unknown as LlmService,
      makeAppConfig(),
    );

    const observer = factory.create("space.1", "TestBot", "member.1");
    expect(observer).toBeInstanceOf(ChatsObserver);
  });

  it("9. Backfill Idempotency: bot reply with reply_to_message_id cancels pending trigger during debounce window", async () => {
    const { ready, pushEvent, llmFake } = setup();

    await ready();

    // 1. Backfill burst: Historical mention from user
    pushEvent("chat.1", {
      ...makeMessage({ id: "hist_1", creator: "user_42" }, { mentionBot: "member_test_bot" }),
      type: "message_added",
    });

    // 2. Immediate anti-trigger: Existing bot response in backfill replying to hist_1
    pushEvent("chat.1", {
      ...makeMessage({
        id: "hist_bot_reply",
        creator: "_participant_space.1_member_test_bot",
        reply_to_message_id: "hist_1",
      }),
      type: "message_added",
    });

    // Wait past debounce window
    await sleep(50);

    // LLM must NOT be called because bot already answered this trigger
    expect(callCount(llmFake.run)).toBe(0);
  });

  it("10. Live Burst: second mention replaces first pending trigger, running LLM only for the latest", async () => {
    const { ready, pushEvent, llmFake } = setup();

    await ready();

    // First mention
    pushEvent("chat.1", {
      ...makeMessage(
        { id: "burst_1", creator: "user_42", content: { text: "first", style: "paragraph" } },
        { mentionBot: "member_test_bot" },
      ),
      type: "message_added",
    });

    // Second mention arrives quickly within debounce window
    pushEvent("chat.1", {
      ...makeMessage(
        { id: "burst_2", creator: "user_42", content: { text: "second", style: "paragraph" } },
        { mentionBot: "member_test_bot" },
      ),
      type: "message_added",
    });

    await waitFor(() => callCount(llmFake.run) === 1);

    expect(callCount(llmFake.run)).toBe(1);
    const ctxArg = callArg<LlmRunContext>(llmFake.run, 0, 0);
    expect(ctxArg.triggerId).toBe("burst_2");
  });

  it("11. Positive Backfill: unanswered mention in replay burst triggers LLM after debounce window", async () => {
    const { ready, pushEvent, anytypeFake, llmFake } = setup();

    await ready();

    // 1. First mention in replay (already answered by bot)
    pushEvent("chat.1", {
      ...makeMessage(
        { id: "hist_answered", creator: "user_42" },
        { mentionBot: "member_test_bot" },
      ),
      type: "message_added",
    });

    // 2. Bot reply in replay that cancels the first mention
    pushEvent("chat.1", {
      ...makeMessage({
        id: "hist_bot_reply",
        creator: "_participant_space.1_member_test_bot",
        reply_to_message_id: "hist_answered",
      }),
      type: "message_added",
    });

    // 3. Second mention in replay (unanswered offline mention)
    pushEvent("chat.1", {
      ...makeMessage(
        {
          id: "hist_unanswered",
          creator: "user_alice",
          content: { text: "offline question", style: "paragraph" },
        },
        { mentionBot: "member_test_bot" },
      ),
      type: "message_added",
    });

    // 4. Intervening noise in replay (does not cancel unanswered mention)
    pushEvent("chat.1", {
      ...makeMessage({
        id: "hist_noise",
        creator: "user_bob",
        content: { text: "just chatting", style: "paragraph" },
      }),
      type: "message_added",
    });

    // Wait past debounce window
    await waitFor(() => callCount(llmFake.run) === 1);

    expect(callCount(llmFake.run)).toBe(1);
    const ctxArg = callArg<LlmRunContext>(llmFake.run, 0, 0);
    expect(ctxArg.triggerId).toBe("hist_unanswered");

    // Check that reply_to_message_id links back to the unanswered question
    const progressPost = callArg<{ text: string; reply_to_message_id?: string }>(
      anytypeFake.addChatMessage,
      0,
      2,
    );
    expect(progressPost.reply_to_message_id).toBe("hist_unanswered");
  });

  it("12. Dynamic Reconciliation: archiving chat unsubscribes SSE, new chat subscribes SSE", async () => {
    let currentChats: Chat[] = [
      { id: "chat.1", name: "Chat 1" } as Chat,
      { id: "chat.2", name: "Chat 2" } as Chat,
    ];

    const { getSseSubject } = setup({
      scanIntervalMs: 25,
      getChats: async () => currentChats,
    });

    await waitFor(
      () => getSseSubject("chat.1") !== undefined && getSseSubject("chat.2") !== undefined,
    );

    const subChat1 = getSseSubject("chat.1");
    const subChat2 = getSseSubject("chat.2");
    expect(subChat1?.observed).toBe(true);
    expect(subChat2?.observed).toBe(true);

    // Archive chat.2, and add chat.3
    currentChats = [
      { id: "chat.1", name: "Chat 1" } as Chat,
      { id: "chat.2", name: "Chat 2", archived: true } as Chat,
      { id: "chat.3", name: "Chat 3" } as Chat,
    ];

    // Wait for scan to reconcile
    await waitFor(() => subChat2?.observed === false);
    await waitFor(() => getSseSubject("chat.3") !== undefined);

    const subChat3 = getSseSubject("chat.3");
    expect(subChat1?.observed).toBe(true);
    expect(subChat2?.observed).toBe(false);
    expect(subChat3?.observed).toBe(true);
  });

  it("13. Self-healing: individual chat software crash revives via retry", async () => {
    let attempts = 0;
    const { ready, pushEvent, llmFake } = setup({
      getChatMessage: async () => {
        attempts++;
        if (attempts === 1) {
          throw new Error("Simulated software error in classifier");
        }
        return makeMessage({
          creator: "_participant_space.1_member_test_bot",
        }) as unknown as ChatMessage;
      },
    });

    await ready();

    // 1. First trigger causes classifier to throw
    pushEvent("chat.1", {
      ...makeMessage({ id: "msg_1", reply_to_message_id: "bot_msg" }),
      type: "message_added",
    });

    // Wait for retry delay (10ms) + buffer
    await sleep(35);

    // 2. Second trigger succeeds on the re-subscribed stream
    pushEvent("chat.1", {
      ...makeMessage({ id: "msg_2", reply_to_message_id: "bot_msg" }),
      type: "message_added",
    });

    await waitFor(() => callCount(llmFake.run) === 1);
    expect(callCount(llmFake.run)).toBe(1);
  });

  it("14. Error handling: LLM error edits progress message with warning and does NOT delete it", async () => {
    const { ready, pushEvent, anytypeFake } = setup({
      llmHandler: () =>
        new Observable((subscriber) => {
          subscriber.error(new Error("Context window exceeded\nTrace: detailed traceback..."));
        }),
    });

    await ready();

    pushEvent("chat.1", {
      ...makeMessage({ id: "msg_err" }, { mentionBot: "member_test_bot" }),
      type: "message_added",
    });

    await waitFor(() => callCount(anytypeFake.editChatMessage) === 1);

    expect(callCount(anytypeFake.addChatMessage)).toBe(1);

    const editArg = callArg<{ text: string; marks: { type: string }[] }>(
      anytypeFake.editChatMessage,
      0,
      3,
    );
    expect(editArg.text).toBe("⚠️ Context window exceeded");
    expect(editArg.text).not.toContain("Trace"); // sanitizer drops everything after the first line
    expect(editArg.marks[0]?.type).toBe("italic");
    expect(callCount(anytypeFake.deleteChatMessage)).toBe(0);
  });

  it("15. Error fallback: when editChatMessage fails on error, progress message is deleted", async () => {
    const { ready, pushEvent, anytypeFake } = setup({
      llmHandler: () =>
        new Observable((subscriber) => {
          subscriber.error(new Error("LLM failure"));
        }),
      editChatMessage: async () => {
        throw new Error("Anytype edit endpoint 500");
      },
    });

    await ready();

    pushEvent("chat.1", {
      ...makeMessage({ id: "msg_fallback" }, { mentionBot: "member_test_bot" }),
      type: "message_added",
    });

    await waitFor(() => callCount(anytypeFake.deleteChatMessage) === 1);
    expect(callCount(anytypeFake.deleteChatMessage)).toBe(1);
  });

  it("16. Empty response: blank LLM response triggers error edit, not hanging working indicator", async () => {
    const { ready, pushEvent, anytypeFake } = setup({
      llmHandler: () => of<LlmEvent>({ type: "response", data: LlmResponse.create("   ") }),
    });

    await ready();

    pushEvent("chat.1", {
      ...makeMessage({ id: "msg_empty" }, { mentionBot: "member_test_bot" }),
      type: "message_added",
    });

    await waitFor(() => callCount(anytypeFake.editChatMessage) === 1);

    const editArg = callArg<{ text: string }>(anytypeFake.editChatMessage, 0, 3);
    expect(editArg.text).toContain("⚠️ Empty response from LLM");
    expect(callCount(anytypeFake.deleteChatMessage)).toBe(0);
  });

  it("17. Incomplete LLM stream: stream with only LlmActions completing without LlmResponse triggers error edit", async () => {
    const { ready, pushEvent, anytypeFake } = setup({
      llmHandler: () => of<LlmEvent>({ type: "actions", data: [LlmAction.create("Thinking...")] }),
    });

    await ready();

    pushEvent("chat.1", {
      ...makeMessage({ id: "msg_incomplete" }, { mentionBot: "member_test_bot" }),
      type: "message_added",
    });

    // 1st edit: LlmAction ("Thinking..."), 2nd edit: Error warning ("⚠️ Empty response from LLM")
    await waitFor(() => callCount(anytypeFake.editChatMessage) === 2);

    const editArg = callArg<{ text: string }>(anytypeFake.editChatMessage, 1, 3);
    expect(editArg.text).toContain("⚠️ Empty response from LLM");
    expect(callCount(anytypeFake.deleteChatMessage)).toBe(0);
  });
});
