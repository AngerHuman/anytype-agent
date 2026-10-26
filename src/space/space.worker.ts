import { defer, from, interval, merge, type Observable, of, timer } from "rxjs";
import { catchError, filter, map, retry, switchMap } from "rxjs/operators";
import type { AnytypeService } from "../client/anytype.service";
import type { SpaceContext, SpaceEvent } from "./schema";

export * from "./schema";

export class SpaceWorker {
  constructor(
    readonly context: SpaceContext,
    private readonly client: AnytypeService,
  ) {}

  /**
   * Combined stream of space events: chat SSE + object mention polling.
   */
  observe(pollIntervalMs = 30000): Observable<SpaceEvent> {
    console.log(
      `👀 [SpaceWorker:${this.context.spaceName}] Initialized observer for space ${this.context.spaceId} (chatId: ${this.context.chatId || "none"}, poll: ${pollIntervalMs}ms)`,
    );

    const streams: Observable<SpaceEvent>[] = [this.observeObjectMentions(pollIntervalMs)];

    if (this.context.chatId) {
      console.log(
        `🔌 [SpaceWorker:${this.context.spaceName}] Registering SSE chat observer for chatId: ${this.context.chatId}`,
      );
      streams.push(this.observeChatSSE(this.context.chatId));
    } else {
      console.warn(
        `⚠️ [SpaceWorker:${this.context.spaceName}] No chatId available. SSE chat observer is disabled!`,
      );
    }

    return merge(...streams);
  }

  // TODO: This should be done differently
  // On receiving a chat message, we must load the chat history to ensure we have previous messages
  // SpaceEvent must be split into SpaceChatEvent & SpaceObjectEvent
  private observeChatSSE(chatId: string): Observable<SpaceEvent> {
    console.log(
      `🔌 [SpaceWorker:${this.context.spaceName}] Subscribing to SSE chat stream (${chatId})...`,
    );

    return this.client.subscribeChatMessages(this.context.spaceId, chatId).pipe(
      map((ssePayload) => {
        const event: SpaceEvent = {
          source: "sse_chat",
          spaceId: this.context.spaceId,
          spaceName: this.context.spaceName,
          payload: ssePayload,
          receivedAt: Date.now(),
        };
        return event;
      }),
      retry({
        delay: (err, retryCount) => {
          console.warn(
            `⚠️ [SpaceWorker:${this.context.spaceName}] SSE chat stream disconnected (${err.message}). Retrying (#${retryCount}) in 5s...`,
          );
          return timer(5000);
        },
      }),
    );
  }

  private observeObjectMentions(pollIntervalMs: number): Observable<SpaceEvent> {
    const query = this.context.botName;

    return interval(pollIntervalMs).pipe(
      switchMap(() =>
        defer(() =>
          this.client.searchSpace(this.context.spaceId, {
            query,
          }),
        ).pipe(
          catchError((err) => {
            console.error(
              `⚠️ [SpaceWorker:${this.context.spaceName}] Object search failed for "${query}": ${err.message}`,
            );
            return of([]);
          }),
        ),
      ),
      switchMap((objects) => from(objects)),
      filter((obj) => {
        // TODO: Add a TypeBox schema validation instead of raw checks
        if (!obj.id) return false;
        if (obj.archived) return false;
        if (this.context.chatId && obj.id === this.context.chatId) return false;
        if (obj.layout === "chat") return false;
        if (obj.name?.toLowerCase().startsWith("chat with ")) return false;
        return true;
      }),
      switchMap((obj) =>
        defer(() => this.client.getObject(this.context.spaceId, obj.id, "md")).pipe(
          map((fullObj) => ({
            searchResult: obj,
            object: fullObj,
          })),
          catchError((err) => {
            console.warn(
              `⚠️ [SpaceWorker:${this.context.spaceName}] Could not fetch body for object ${obj.id}: ${err.message}`,
            );
            return of({
              searchResult: obj,
              object: null,
            });
          }),
        ),
      ),
      map((combinedPayload) => {
        console.log(
          `📑 [SpaceWorker:${this.context.spaceName}] Found object mentioning bot: "${combinedPayload.searchResult.name || "Untitled"}" (${combinedPayload.searchResult.id})`,
        );
        const event: SpaceEvent = {
          source: "object_search",
          spaceId: this.context.spaceId,
          spaceName: this.context.spaceName,
          payload: combinedPayload,
          receivedAt: Date.now(),
        };
        return event;
      }),
    );
  }
}
