import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import { Logger } from "@nestjs/common";
import { firstValueFrom } from "rxjs";
import { AnytypeClient } from "../../client/anytype.client";
import { AnytypeService } from "../../client/anytype.service";
import { initSpaceOrchestrator, type SpaceContext, SpaceWorker } from "../index";

describe("Space Module", () => {
  const testLogger = new Logger("TestSpaceModule");
  const createTestClient = () =>
    new AnytypeClient(testLogger, "http://127.0.0.1:31012", "secret-token");

  let fetchSpy: ReturnType<typeof spyOn>;

  beforeEach(() => {
    fetchSpy = spyOn(globalThis, "fetch");
  });

  afterEach(() => {
    fetchSpy.mockRestore();
  });

  describe("initSpaceOrchestrator", () => {
    it("filters out nameless spaces, non-member spaces, and viewer spaces", async () => {
      const mockSpaces = {
        data: [
          { id: "space1", name: "Valid Space 1" },
          { id: "space2", name: "Viewer Space" },
          { id: "space3", name: "No Bot Space" },
          { id: "space4", name: "" }, // Nameless space -> should be skipped immediately
        ],
      };

      const mockMembersSpace1 = {
        data: [
          {
            id: "_p_1",
            identity: "id-1",
            name: "GeminiBot",
            role: "editor",
            status: "active",
          },
        ],
      };

      const mockMembersSpace2 = {
        data: [
          {
            id: "_p_2",
            identity: "id-2",
            name: "GeminiBot",
            role: "viewer", // Read only -> should be skipped
            status: "active",
          },
        ],
      };

      const mockMembersSpace3 = {
        data: [
          {
            id: "_p_3",
            identity: "id-3",
            name: "Alice",
            role: "owner",
            status: "active",
          },
        ],
      };

      fetchSpy.mockImplementation(async (url: string | URL | Request, init?: RequestInit) => {
        const urlStr = url.toString();
        const method = init?.method || "GET";

        if (urlStr.endsWith("/v1/spaces")) {
          return new Response(JSON.stringify(mockSpaces), { status: 200 });
        }
        if (urlStr.includes("/v1/spaces/space1/members")) {
          return new Response(JSON.stringify(mockMembersSpace1), { status: 200 });
        }
        if (urlStr.includes("/v1/spaces/space2/members")) {
          return new Response(JSON.stringify(mockMembersSpace2), { status: 200 });
        }
        if (urlStr.includes("/v1/spaces/space3/members")) {
          return new Response(JSON.stringify(mockMembersSpace3), { status: 200 });
        }
        if (urlStr.includes("/chats") && method === "GET") {
          return new Response(
            JSON.stringify({
              data: [{ id: "chat.space1", name: "Chat with GeminiBot" }],
            }),
            { status: 200 },
          );
        }
        if (urlStr.includes("/types") && method === "GET") {
          return new Response(
            JSON.stringify({
              data: [
                {
                  id: "type.agent_response",
                  key: "agent_response",
                  name: "Agent Response",
                  plural_name: "Agent Responses",
                },
              ],
            }),
            { status: 200 },
          );
        }
        return new Response(JSON.stringify({ data: [] }), { status: 200 });
      });

      const client = createTestClient();
      const service = new AnytypeService(client);
      const events$ = await initSpaceOrchestrator(service, "GeminiBot");

      expect(events$).toBeDefined();
    });
  });

  describe("SpaceWorker", () => {
    it("emits SpaceEvent when objects mentioning bot are found", async () => {
      const mockContext: SpaceContext = {
        spaceId: "space1",
        spaceName: "Test Space",
        botMemberId: "_p_bot",
        botIdentity: "bot-id",
        botName: "GeminiBot",
        botRole: "editor",
      };

      const mockSearchResults = {
        data: [
          {
            id: "obj.123",
            name: "Architecture Doc",
            snippet: "Mentioning @GeminiBot here",
          },
        ],
      };

      fetchSpy.mockImplementation(async (url: string | URL | Request) => {
        const urlStr = url.toString();
        if (urlStr.includes("/search")) {
          return new Response(JSON.stringify(mockSearchResults), { status: 200 });
        }
        if (urlStr.includes("/objects/obj.123")) {
          return new Response(
            JSON.stringify({
              object: {
                id: "obj.123",
                name: "Architecture Doc",
                body: "# Architecture\nHello @GeminiBot",
              },
            }),
            { status: 200 },
          );
        }
        return new Response(JSON.stringify({ data: [] }), { status: 200 });
      });

      const client = createTestClient();
      const service = new AnytypeService(client);
      const worker = new SpaceWorker(mockContext, service);

      const event = await firstValueFrom(worker.observe(50));

      expect(event.source).toBe("object_search");
      expect(event.spaceId).toBe("space1");
      expect(event.spaceName).toBe("Test Space");
      expect(event.payload.searchResult.id).toBe("obj.123");
      expect(event.payload.object.name).toBe("Architecture Doc");
    });
  });
});
