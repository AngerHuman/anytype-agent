import "reflect-metadata";
import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import { Test, type TestingModule } from "@nestjs/testing";
import { of } from "rxjs";
import { makeAppConfig } from "../../config/__tests__/fixtures";
import { APP_CONFIG, ConfigModule } from "../../config/config.module";
import { LlmService } from "../../llm/llm.service";
import { LlmResponse } from "../../llm/types";
import { ObserverModule } from "../observer.module";
import { ObserverService } from "../observer.service";
import {
  callCount,
  extractChatId,
  extractSpaceId,
  makeMessage,
  type ObserverServiceInternals,
  sleep,
  waitFor,
} from "./helpers";

describe("ObserverModule (Integration Tests via Nest Test)", () => {
  const originalEnv = { ...process.env };
  let testingModule: TestingModule;
  let observerService: ObserverService;
  let internals: ObserverServiceInternals;

  let runSpy: ReturnType<typeof spyOn>;
  let fetchSpy: ReturnType<typeof spyOn>;

  // Mutable state for routing fake Anytype API
  let spacesList: Array<{ id: string; name: string }> = [];
  let membersMap: Record<string, unknown[]> = {};
  let chatsMap: Record<string, Array<{ id: string; name: string }>> = {};
  let chatsStatus = 200;

  const postMessageCalls: Array<{ space_id: string; chat_id: string; text?: string }> = [];
  const sseControllers: ReadableStreamDefaultController<Uint8Array>[] = [];
  const encoder = new TextEncoder();

  const pushSseEvent = (
    controller: ReadableStreamDefaultController<Uint8Array>,
    eventType: string,
    payload: unknown,
  ) => {
    const chunk = `event: ${eventType}\ndata: ${JSON.stringify(payload)}\n\n`;
    controller.enqueue(encoder.encode(chunk));
  };

  beforeAll(async () => {
    // Mock LLM calls prior to compile() to avoid triggering real SSH/CLI
    runSpy = spyOn(LlmService.prototype, "run").mockImplementation(() =>
      of({ type: "response", data: LlmResponse.create("  Bot reply  ") }),
    );

    spacesList = [
      { id: "sp_main", name: "Main Space" },
      { id: "sp_viewer", name: "Viewer Space" },
      { id: "sp_empty", name: "" },
    ];

    membersMap = {
      sp_main: [
        { id: "mem_1", identity: "TestBot", name: "TestBot", role: "editor", status: "active" },
      ],
      sp_viewer: [
        { id: "mem_2", identity: "TestBot", name: "TestBot", role: "viewer", status: "active" },
      ],
      sp_empty: [
        { id: "mem_3", identity: "TestBot", name: "TestBot", role: "editor", status: "active" },
      ],
    };

    chatsMap = {
      sp_main: [{ id: "chat_main", name: "Chat with TestBot" }],
    };

    fetchSpy = spyOn(globalThis, "fetch").mockImplementation((async (
      input: string | URL | Request,
      init?: RequestInit,
    ) => {
      const url = String(input);
      const method = init?.method ?? "GET";

      if (url.includes("/messages/stream")) {
        const stream = new ReadableStream<Uint8Array>({
          start(controller) {
            sseControllers.push(controller);
          },
        });
        return new Response(stream, {
          status: 200,
          headers: { "Content-Type": "text/event-stream" },
        });
      }

      if (url.includes("/messages") && method === "GET") {
        if (url.match(/\/messages\/[^/?]+$/)) {
          return new Response(
            JSON.stringify({
              message: {
                id: "m_1",
                creator: "_participant_sp_main_other",
                created_at: Date.now(),
                content: { text: "hi", style: "paragraph" },
              },
            }),
            { status: 200 },
          );
        }
        return new Response(JSON.stringify({ messages: [] }), { status: 200 });
      }

      if (url.endsWith("/messages") && method === "POST") {
        const body = typeof init?.body === "string" ? JSON.parse(init.body) : init?.body;
        const spaceId = extractSpaceId(url);
        const chatId = extractChatId(url);
        postMessageCalls.push({
          space_id: spaceId,
          chat_id: chatId,
          text: (body as { text: string })?.text,
        });
        return new Response(JSON.stringify({ message_id: "m_1" }), { status: 200 });
      }

      if (method === "PATCH" || method === "DELETE") {
        return new Response(JSON.stringify({}), { status: 200 });
      }

      if (url.includes("/chats") && method === "GET") {
        if (chatsStatus !== 200) {
          return new Response(JSON.stringify({ error: "Server Error" }), { status: chatsStatus });
        }
        const spaceId = extractSpaceId(url);
        const chats = chatsMap[spaceId] ?? [];
        return new Response(JSON.stringify({ data: chats }), { status: 200 });
      }

      if (url.includes("/members") && method === "GET") {
        const spaceId = extractSpaceId(url);
        const members = membersMap[spaceId] ?? [];
        return new Response(JSON.stringify({ data: members }), { status: 200 });
      }

      if (url.endsWith("/spaces") && method === "GET") {
        return new Response(JSON.stringify({ data: spacesList }), { status: 200 });
      }

      return new Response(JSON.stringify({ data: [] }), { status: 200 });
    }) as unknown as typeof fetch);

    testingModule = await Test.createTestingModule({
      imports: [ConfigModule, ObserverModule],
    })
      .overrideProvider(APP_CONFIG)
      .useValue(
        makeAppConfig({
          SETTINGS: { DEBOUNCE_MS: 10, RETRY_DELAY_MS: 10, SCAN_INTERVAL_MS: 200 },
        }),
      )
      .compile();

    await testingModule.init();
    observerService = testingModule.get(ObserverService);
    internals = observerService as unknown as ObserverServiceInternals;
  });

  afterAll(async () => {
    await testingModule?.close();
    fetchSpy?.mockRestore();
    runSpy?.mockRestore();
    process.env = originalEnv;
  });

  it("1. Space with bot-editor: observer appears in registry", async () => {
    await waitFor(() => internals.registry.has("sp_main"));
    expect(internals.registry.has("sp_main")).toBe(true);
  });

  it("2. Viewer space and nameless space: do not appear in registry", async () => {
    expect(internals.registry.has("sp_viewer")).toBe(false);
    expect(internals.registry.has("sp_empty")).toBe(false);
  });

  it("3. Full cycle: enqueue user message_added -> LLM called, POST /messages sent with 'Bot reply', lastActivity updated", async () => {
    await waitFor(() => sseControllers.length >= 1);
    const controller = sseControllers[sseControllers.length - 1];
    if (!controller) throw new Error("SSE controller not found");

    const initialLlmCalls = callCount(runSpy);

    pushSseEvent(controller, "message_added", {
      type: "message_added",
      payload: { message: makeMessage({ creator_name: "Alice", id: "init_msg" }) },
    });
    await sleep(40);

    pushSseEvent(controller, "message_added", {
      type: "message_added",
      payload: {
        message: makeMessage({ creator_name: "Alice", id: "real_msg" }, { mentionBot: "TestBot" }),
      },
    });

    await waitFor(() => callCount(runSpy) === initialLlmCalls + 1);

    await waitFor(() => postMessageCalls.length >= 1);
    const lastPost = postMessageCalls[postMessageCalls.length - 1];
    if (!lastPost) throw new Error("No POST call recorded");
    expect(lastPost.text).toBe("Bot reply");
    expect(lastPost.chat_id).toBe("chat_main");
    expect(lastPost.space_id).toBe("sp_main");

    expect(internals.lastActivity.has("sp_main")).toBe(true);
    expect(typeof internals.lastActivity.get("sp_main")).toBe("number");
  });

  it("4. Anti-echo: enqueue message_added from TestBot -> run is not called", async () => {
    const controller = sseControllers[sseControllers.length - 1];
    if (!controller) throw new Error("SSE controller not found");
    const initialLlmCalls = callCount(runSpy);

    pushSseEvent(controller, "message_added", {
      type: "message_added",
      payload: {
        message: makeMessage({ creator: "_participant_sp_main_TestBot", creator_name: "TestBot" }),
      },
    });
    pushSseEvent(controller, "message_added", {
      type: "message_added",
      payload: {
        message: makeMessage({ creator: "_participant_sp_main_TestBot", creator_name: "testbot" }),
      },
    });

    await sleep(50);
    expect(callCount(runSpy)).toBe(initialLlmCalls);
  });

  it("5. Fatal error and recovery: GET /chats 500 -> registry cleared; return 200 -> next scan recreates observer", async () => {
    expect(internals.registry.has("sp_main")).toBe(true);

    chatsStatus = 500;
    // SSE error -> reconnect/recreate
    const ctrl = sseControllers[sseControllers.length - 1];
    if (ctrl) {
      try {
        ctrl.error(new Error("SSE dropped"));
      } catch {}
    }

    // Remove from registry; scan will attempt to recreate while /chats is broken
    internals.registry.get("sp_main")?.forEach((obs) => {
      obs.destroy();
    });
    internals.registry.delete("sp_main");

    // Scan hits /chats 500 -> fatal -> registry empty
    await sleep(250);
    expect(internals.registry.has("sp_main")).toBe(false);

    chatsStatus = 200;

    await waitFor(() => internals.registry.has("sp_main"), 2500);
    expect(internals.registry.has("sp_main")).toBe(true);
  });

  it("6. Disappeared space: /v1/spaces empty -> registry empty", async () => {
    expect(internals.registry.has("sp_main")).toBe(true);

    spacesList = [];

    await waitFor(() => !internals.registry.has("sp_main"));
    expect(internals.registry.has("sp_main")).toBe(false);
  });
});
