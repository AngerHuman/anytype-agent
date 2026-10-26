import "reflect-metadata";
import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import { Logger } from "@nestjs/common";
import { NestFactory } from "@nestjs/core";
import { firstValueFrom } from "rxjs";
import { Type } from "typebox";
import { makeAppEnv } from "../../config/__tests__/fixtures";
import { ConfigModule } from "../../config/config.module";
import { AnytypeClient } from "../anytype.client";
import { AnytypeService } from "../anytype.service";
import { ANYTYPE_CLIENT } from "../client.constants";
import { ClientModule } from "../client.module";
import type { ChatMessage } from "../schema";

describe("Anytype Client & Service Layer (Separation of Concerns)", () => {
  const testLogger = new Logger("TestAnytypeClient");
  const originalEnv = { ...process.env };
  const createTestClient = (
    baseUrl = "http://127.0.0.1:31012",
    apiKey = "secret-token-xyz",
    apiVersion = "2025-11-08",
  ) => new AnytypeClient(testLogger, baseUrl, apiKey, apiVersion);

  let fetchSpy: ReturnType<typeof spyOn>;

  beforeEach(() => {
    fetchSpy = spyOn(globalThis, "fetch");
  });

  afterEach(() => {
    fetchSpy.mockRestore();
    process.env = originalEnv;
  });

  describe("AnytypeClient (Low-Level Transport)", () => {
    it("normalizes baseUrl and sends standard HTTP headers", async () => {
      fetchSpy.mockResolvedValue(
        new Response(JSON.stringify({ status: "ok" }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
      );

      const client = createTestClient();
      expect(client.baseUrl).toBe("http://127.0.0.1:31012");

      const res = await client.get(Type.Object({ status: Type.String() }), "/ping");
      expect(res.status).toBe("ok");

      expect(fetchSpy).toHaveBeenCalledTimes(1);
      const [url, init] = fetchSpy.mock.calls[0] as [string, RequestInit];
      expect(url).toBe("http://127.0.0.1:31012/ping");
      expect(init.headers).toEqual({
        "Content-Type": "application/json",
        "Anytype-Version": "2025-11-08",
        Authorization: "Bearer secret-token-xyz",
      });
    });

    it("stream: connects to SSE stream and emits raw parsed events", async () => {
      const ssePayload =
        'event: message_added\ndata: {"payload":{"message":{"id":"msg1","text":"hello"}}}\n\n';

      const mockStream = new ReadableStream({
        start(controller) {
          controller.enqueue(new TextEncoder().encode(ssePayload));
          controller.close();
        },
      });

      fetchSpy.mockResolvedValue(
        new Response(mockStream, {
          status: 200,
          headers: { "Content-Type": "text/event-stream" },
        }),
      );

      const client = createTestClient();
      const stream$ = client.stream("/v1/events/stream");

      const event = await firstValueFrom(stream$);

      expect(event.event).toBe("message_added");
      const eventData = event.data as { payload?: { message?: { text?: string } } };
      expect(eventData?.payload?.message?.text).toBe("hello");
    });

    it("checkHealth: succeeds when endpoint responds with 200", async () => {
      fetchSpy.mockResolvedValue(new Response(JSON.stringify({ data: [] }), { status: 200 }));

      const client = createTestClient();
      expect(client.checkHealth("/v1/spaces", 3, 10)).resolves.toBeUndefined();
    });

    it("checkHealth: fails immediately without retries on 401 Unauthorized", async () => {
      fetchSpy.mockResolvedValue(
        new Response("Unauthorized", { status: 401, statusText: "Unauthorized" }),
      );

      const client = createTestClient();
      expect(client.checkHealth("/v1/spaces", 5, 10)).rejects.toThrow(
        "Authentication failed with status 401",
      );
      expect(fetchSpy).toHaveBeenCalledTimes(1); // No wasteful retries on bad credentials
    });
  });

  describe("AnytypeService (Domain API Facade)", () => {
    it("getSpaces: fetches spaces list through client", async () => {
      fetchSpy.mockResolvedValue(
        new Response(
          JSON.stringify({
            data: [{ id: "space.1", name: "Test Space" }],
          }),
          { status: 200 },
        ),
      );

      const client = createTestClient();
      const service = new AnytypeService(client);

      const spaces = await service.getSpaces();
      expect(spaces).toHaveLength(1);
      expect(spaces[0]?.name).toBe("Test Space");
    });

    it("createChat: sends body to space chats endpoint", async () => {
      fetchSpy.mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            chat: { id: "chat_1", name: "New Chat" },
          }),
          { status: 200 },
        ),
      );

      const client = createTestClient();
      const service = new AnytypeService(client);

      const chat = await service.createChat("space_99", {
        name: "New Chat",
      });

      expect(chat.id).toBe("chat_1");
      const [url, init] = fetchSpy.mock.calls[0] as [string, RequestInit];
      expect(url).toBe("http://127.0.0.1:31012/v1/spaces/space_99/chats");
      expect(init.method).toBe("POST");
      expect(JSON.parse(init.body as string)).toEqual({
        name: "New Chat",
      });
    });

    it("addChatMessage: sends message body to correct chat endpoint", async () => {
      fetchSpy.mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            message_id: "msg_123",
          }),
          { status: 200 },
        ),
      );

      const client = createTestClient();
      const service = new AnytypeService(client);

      const res = await service.addChatMessage("space_99", "chat_42", {
        text: "Hi from bot",
      });

      expect(res.message_id).toBe("msg_123");
      const [url, init] = fetchSpy.mock.calls[0] as [string, RequestInit];
      expect(url).toBe("http://127.0.0.1:31012/v1/spaces/space_99/chats/chat_42/messages");
      expect(JSON.parse(init.body as string)).toEqual({
        text: "Hi from bot",
      });
    });

    it("editChatMessage: sends PATCH with text and optional attributes to message endpoint", async () => {
      fetchSpy.mockResolvedValueOnce(new Response(JSON.stringify({}), { status: 200 }));

      const client = createTestClient();
      const service = new AnytypeService(client);

      const res = await service.editChatMessage("space_99", "chat_42", "msg_777", {
        text: "Updated text",
        style: "paragraph",
        marks: [{ type: "bold", from: 0, to: 5 }],
      });

      expect(res).toEqual({});
      const [url, init] = fetchSpy.mock.calls[0] as [string, RequestInit];
      expect(url).toBe("http://127.0.0.1:31012/v1/spaces/space_99/chats/chat_42/messages/msg_777");
      expect(init.method).toBe("PATCH");
      expect(JSON.parse(init.body as string)).toEqual({
        text: "Updated text",
        style: "paragraph",
        marks: [{ type: "bold", from: 0, to: 5 }],
      });
    });

    it("deleteChatMessage: sends DELETE to message endpoint", async () => {
      fetchSpy.mockResolvedValueOnce(new Response(JSON.stringify({}), { status: 200 }));

      const client = createTestClient();
      const service = new AnytypeService(client);

      const res = await service.deleteChatMessage("space_99", "chat_42", "msg_777");

      expect(res).toEqual({});
      const [url, init] = fetchSpy.mock.calls[0] as [string, RequestInit];
      expect(url).toBe("http://127.0.0.1:31012/v1/spaces/space_99/chats/chat_42/messages/msg_777");
      expect(init.method).toBe("DELETE");
    });

    it("getChatMessage: fetches single message from message endpoint", async () => {
      const mockMsg = {
        id: "msg_777",
        creator: "user_123",
        created_at: 1000,
        content: { text: "Hello", style: "paragraph" },
      };
      fetchSpy.mockResolvedValueOnce(
        new Response(JSON.stringify({ message: mockMsg }), { status: 200 }),
      );

      const client = createTestClient();
      const service = new AnytypeService(client);

      const res = await service.getChatMessage("space_99", "chat_42", "msg_777");

      expect(res).toEqual(mockMsg as unknown as ChatMessage);
      const [url, init] = fetchSpy.mock.calls[0] as [string, RequestInit];
      expect(url).toBe("http://127.0.0.1:31012/v1/spaces/space_99/chats/chat_42/messages/msg_777");
      expect(init.method).toBe("GET");
    });

    it("toggleMessageReaction: sends POST with emoji to reactions endpoint", async () => {
      fetchSpy.mockResolvedValueOnce(new Response(JSON.stringify({}), { status: 200 }));

      const client = createTestClient();
      const service = new AnytypeService(client);

      const res = await service.toggleMessageReaction("space_99", "chat_42", "msg_777", {
        emoji: "👍",
      });

      expect(res).toEqual({});
      const [url, init] = fetchSpy.mock.calls[0] as [string, RequestInit];
      expect(url).toBe(
        "http://127.0.0.1:31012/v1/spaces/space_99/chats/chat_42/messages/msg_777/reactions",
      );
      expect(init.method).toBe("POST");
      expect(JSON.parse(init.body as string)).toEqual({
        emoji: "👍",
      });
    });

    it("createType: sends body to space types endpoint", async () => {
      fetchSpy.mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            type: { id: "type_1", key: "custom", name: "Custom", plural_name: "Customs" },
          }),
          { status: 200 },
        ),
      );

      const client = createTestClient();
      const service = new AnytypeService(client);

      const res = await service.createType("space_99", {
        name: "Custom",
        plural_name: "Customs",
        layout: "basic",
      });

      expect(res.id).toBe("type_1");
      const [url, init] = fetchSpy.mock.calls[0] as [string, RequestInit];
      expect(url).toBe("http://127.0.0.1:31012/v1/spaces/space_99/types");
      expect(init.method).toBe("POST");
      expect(JSON.parse(init.body as string)).toEqual({
        name: "Custom",
        plural_name: "Customs",
        layout: "basic",
      });
    });

    it("empty 200 body: mutations succeed when backend returns no body at all", async () => {
      fetchSpy.mockResolvedValueOnce(new Response("", { status: 200 }));

      const client = createTestClient();
      const service = new AnytypeService(client);

      const res = await service.deleteChatMessage("space_99", "chat_42", "msg_777");

      expect(res).toEqual({});
    });
  });

  describe("ClientModule DI with Async Factory Provider", () => {
    it("bootstraps ClientModule, performs healthcheck in useFactory, and provides ANYTYPE_CLIENT & AnytypeService", async () => {
      Object.assign(
        process.env,
        makeAppEnv({ ANYTYPE__BOT_NAME: "Bot", ANYTYPE__API_KEY: "token123" }),
      );

      fetchSpy.mockResolvedValue(new Response(JSON.stringify({ data: [] }), { status: 200 }));

      const app = await NestFactory.createApplicationContext(
        {
          module: class TestAppModule {},
          imports: [ConfigModule, ClientModule],
        },
        { logger: false },
      );

      const client = app.get<AnytypeClient>(ANYTYPE_CLIENT);
      const service = app.get(AnytypeService);

      expect(client).toBeInstanceOf(AnytypeClient);
      expect(service).toBeInstanceOf(AnytypeService);
      expect(client.baseUrl).toBe("http://127.0.0.1:31012");

      await app.close();
    });
  });
});
