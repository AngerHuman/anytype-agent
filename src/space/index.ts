import { merge, type Observable, of } from "rxjs";
import type { AnytypeService } from "../client/anytype.service";
import type { Member, Space } from "../client/schema";
import type { SpaceContext, SpaceEvent } from "./schema";
import { SpaceWorker } from "./space.worker";

export * from "./schema";
export { SpaceWorker };

//TODO: decouple this to multiple functions, use rxJS flows
async function setupSpaceContext(
  client: AnytypeService,
  space: Space,
  botMember: Member,
): Promise<SpaceContext> {
  console.log(`🔧 [Orchestrator] Setting up resources for space: "${space.name}" (${space.id})`);

  let chatId: string | undefined;
  try {
    console.log(`🔍 [Space:${space.name}] Fetching existing chats for bot "${botMember.name}"...`);
    const chats = await client.getChats(space.id);
    console.log(
      `📋 [Space:${space.name}] Retrieved ${chats.length} chat(s):`,
      chats.map((c) => `"${c.name}" (${c.id})`),
    );

    // TODO: After SQLite is done, check only by chatId, not by name
    const existingChat = chats.find(
      (c) =>
        c.name?.toLowerCase() === `chat with ${botMember.name}`.toLowerCase() ||
        c.name?.toLowerCase() === botMember.name.toLowerCase() ||
        c.name?.toLowerCase() === "gemini" ||
        c.name?.toLowerCase().includes(botMember.name.toLowerCase()),
    );

    if (existingChat) {
      chatId = existingChat.id;
      console.log(
        `💬 [Space:${space.name}] Matched existing chat: "${existingChat.name}" (${chatId})`,
      );
    } else {
      console.log(
        `💬 [Space:${space.name}] No bot chat found. Creating "Chat with ${botMember.name}"...`,
      );
      const newChat = await client.createChat(space.id, {
        name: `Chat with ${botMember.name}`,
      });
      chatId = newChat.id;
      console.log(`✅ [Space:${space.name}] Chat created successfully: ${chatId}`);
    }
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    console.error(`⚠️ [Space:${space.name}] Failed to get/create chat via Chats API: ${message}`);
    try {
      console.log(
        `🔎 [Space:${space.name}] Attempting fallback object search for "Chat with ${botMember.name}"...`,
      );
      const results = await client.searchSpace(space.id, { query: `Chat with ${botMember.name}` });
      const chatObj = results.find(
        (o) =>
          o.name?.toLowerCase().includes("chat") &&
          o.name?.toLowerCase().includes(botMember.name.toLowerCase()),
      );
      if (chatObj) {
        chatId = chatObj.id;
        console.log(
          `🎯 [Space:${space.name}] Found chat object via fallback search: "${chatObj.name}" (${chatId})`,
        );
      }
    } catch (fallbackErr: unknown) {
      const fallbackMsg = fallbackErr instanceof Error ? fallbackErr.message : String(fallbackErr);
      console.error(`❌ [Space:${space.name}] Fallback search failed: ${fallbackMsg}`);
    }
  }

  // "Agent Response" is an optional enhancement: creation error is non-fatal
  //TODO: double check type creation logic, cover it with tests
  let agentResponseTypeId: string | undefined;
  try {
    const types = await client.getTypes(space.id);
    const existingType = types.find(
      (t) => t.name === "Agent Response" || t.key === "agent_response",
    );

    if (existingType) {
      agentResponseTypeId = existingType.id;
    } else {
      console.log(`📄 [Space:${space.name}] Creating custom type "Agent Response"...`);
      const newType = await client.createType(space.id, {
        name: "Agent Response",
        plural_name: "Agent Responses",
        layout: "note",
        key: "agent_response",
      });
      agentResponseTypeId = newType.id;
    }
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    console.warn(
      `⚠️ [Space:${space.name}] Could not verify/create "Agent Response" type: ${message}`,
    );
  }

  return {
    spaceId: space.id,
    spaceName: space.name,
    botMemberId: botMember.id,
    botIdentity: botMember.identity,
    botName: botMember.name,
    botRole: botMember.role,
    chatId,
    agentResponseTypeId,
  };
}

/**
 * Returns merged Observable of events across all spaces where the bot passed verification.
 */
export async function initSpaceOrchestrator(
  client: AnytypeService,
  botIdentifier: string,
): Promise<Observable<SpaceEvent>> {
  console.log(`🌐 [Orchestrator] Scanning spaces for bot: "${botIdentifier}"...`);
  const allSpaces = await client.getSpaces();
  const validContexts: SpaceContext[] = [];

  for (const space of allSpaces) {
    if (!space.name || space.name.trim() === "") {
      // Nameless / internal / one-to-one chat spaces are ignored
      continue;
    }

    //TODO: Move to separate function (verifyBot or smth)

    try {
      const members = await client.getMembers(space.id);
      const bot = members.find(
        (m) => m.name.toLowerCase() === botIdentifier.toLowerCase() || m.identity === botIdentifier,
      );

      if (!bot) {
        continue;
      }

      if (bot.status !== "active") {
        console.warn(
          `⚠️ [Space:${space.name}] Bot is in status "${bot.status}" (not active), skipping.`,
        );
        continue;
      }

      // Strict role check: viewer or no_permission cannot mutate objects
      if (bot.role === "viewer" || bot.role === "no_permission") {
        console.warn(
          `⛔ [Space:${space.name}] Bot has role "${bot.role}" (read-only). Skipping space to prevent mutation failures.`,
        );
        continue;
      }

      console.log(`✅ [Space:${space.name}] Bot verified as active with role "${bot.role}".`);

      const context = await setupSpaceContext(client, space, bot);
      validContexts.push(context);
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      console.error(`❌ [Space:${space.name}] Failed to inspect space: ${message}`);
    }
  }

  if (validContexts.length === 0) {
    console.warn(
      `⚠️ [Orchestrator] No valid spaces found where bot "${botIdentifier}" is an active editor/admin/owner!`,
    );
    return of();
  }

  console.log(`🚀 [Orchestrator] Initializing ${validContexts.length} SpaceWorker(s)...`);

  const workers = validContexts.map((ctx) => new SpaceWorker(ctx, client));
  const streams = workers.map((w) => w.observe());

  return merge(...streams);
}
