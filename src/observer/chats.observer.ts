import { Inject, Injectable } from "@nestjs/common";
import {
  catchError,
  concatMap,
  connect,
  debounceTime,
  defer,
  distinctUntilChanged,
  EMPTY,
  exhaustMap,
  filter,
  from,
  ignoreElements,
  type MonoTypeOperatorFunction,
  map,
  merge,
  mergeMap,
  Observable,
  of,
  retry,
  scan,
  shareReplay,
  switchMap,
  takeUntil,
  tap,
  throwIfEmpty,
  timeout,
  timer,
} from "rxjs";
import { AnytypeService } from "../client/anytype.service";
import type { ChatMessage } from "../client/schema";
import type { ChatMessageAdded } from "../client/types";
import { APP_CONFIG } from "../config/config.module";
import type { AppConfig } from "../config/config.schema";
import { LlmService } from "../llm/llm.service";
import { LlmAction, LlmEmptyResponseError, type LlmEvent } from "../llm/types";
import { AbstractObserver, type ObserverFactory } from "./types";

@Injectable()
export class ChatsObserverFactory implements ObserverFactory {
  constructor(
    private readonly anytype: AnytypeService,
    private readonly llm: LlmService,
    @Inject(APP_CONFIG) private readonly config: AppConfig,
  ) {}

  create(spaceId: string, _botName: string, botMemberId: string): ChatsObserver {
    return new ChatsObserver(spaceId, botMemberId, this.anytype, this.llm, this.config);
  }
}

// TODO; move to class itself
const INITIAL_PROGRESS_TEXT = "⏳ Working...";
const PROGRESS_POST_TIMEOUT_MS = 5_000;
const PROGRESS_RETRY_DELAY_MS = 1_000;
const SAFE_HTTP_TIMEOUT_MS = 15_000;

type ChatIntent =
  | { type: "trigger"; msg: ChatMessageAdded }
  | { type: "bot_reply"; replyToId?: string }
  | { type: "noop" };

export class ChatsObserver extends AbstractObserver {
  constructor(
    private readonly spaceId: string,
    private readonly botMemberId: string,
    private readonly anytype: AnytypeService,
    private readonly llm: LlmService,
    private readonly config: AppConfig,
  ) {
    super();
  }

  run(): Observable<unknown> {
    const scanIntervalMs = this.config.SETTINGS.SCAN_INTERVAL_MS;

    const activeChatIds$ = timer(0, scanIntervalMs).pipe(
      exhaustMap(() => this.anytype.getChats(this.spaceId)),
      map((chats) => new Set(chats.filter((c) => !c.archived).map((c) => c.id))),
      shareReplay({ bufferSize: 1, refCount: true }),
    );

    return activeChatIds$.pipe(
      scan(
        (acc, activeIds) => ({
          added: [...activeIds].filter((id) => !acc.knownIds.has(id)),
          knownIds: activeIds,
        }),
        { added: [] as string[], knownIds: new Set<string>() },
      ),
      mergeMap(({ added }) => from(added)),
      mergeMap((chatId) =>
        this.subscribeToChat(chatId).pipe(
          retry({
            delay: (err) => {
              this.logger.error(
                `Failed in chat ${chatId}: ${this.sanitizeErrorMessage(err)}`,
                err?.stack,
              );
              return timer(this.config.SETTINGS.RETRY_DELAY_MS);
            },
          }),
          takeUntil(activeChatIds$.pipe(filter((activeIds) => !activeIds.has(chatId)))),
        ),
      ),
      takeUntil(this.destroy$),
    );
  }

  private subscribeToChat(chatId: string): Observable<unknown> {
    const intent$ = this.anytype.subscribeChatMessages(this.spaceId, chatId).pipe(
      retry({ delay: this.config.SETTINGS.RETRY_DELAY_MS }),
      filter((msg) => msg !== null && msg.type === "message_added"),
      concatMap((msg) => this.classifyMessage(chatId, msg)),
      scan((pending: ChatMessageAdded | null, intent: ChatIntent): ChatMessageAdded | null => {
        if (intent.type === "bot_reply") {
          return pending && intent.replyToId === pending.id ? null : pending;
        }
        if (intent.type === "trigger") {
          return intent.msg;
        }
        return pending;
      }, null),
      distinctUntilChanged((a, b) => a?.id === b?.id),
      debounceTime(this.config.SETTINGS.DEBOUNCE_MS),
      filter((msg): msg is ChatMessageAdded => msg !== null),
    );

    return intent$.pipe(switchMap((msg) => this.handleTrigger(chatId, msg)));
  }

  private handleTrigger(chatId: string, msg: ChatMessageAdded): Observable<unknown> {
    this.logger.log(
      `💬 Generating LLM response for chat ${chatId} (trigger: ${msg.id}, creator: ${msg.creator}): "${msg.content?.text?.slice(0, 60)}"...`,
    );

    return this.createProgressMessage(chatId, msg.id).pipe(
      switchMap((progressId) => {
        const abortController = new AbortController();

        return this.request$(() => this.anytype.getChatMessages(this.spaceId, chatId)).pipe(
          switchMap((history) =>
            this.processLlmRun(chatId, history, progressId, abortController.signal, msg.id),
          ),
          catchError((err) => {
            if (!abortController.signal.aborted) abortController.abort();

            return this.handleLlmError(chatId, progressId, err);
          }),
          tap({
            unsubscribe: () => {
              this.logger.warn(
                `🛑 Preempted/Cancelled trigger ${msg.id} in chat ${chatId} (progress ${progressId})`,
              );
              if (!abortController.signal.aborted) abortController.abort();

              this.deleteProgressMessage(chatId, progressId).subscribe();
            },
          }),
        );
      }),
      catchError((err) => {
        const errMsg = this.sanitizeErrorMessage(err);
        this.logger.error(`❌ Failed to initialize trigger for chat ${chatId}: ${errMsg}`);
        return EMPTY;
      }),
    );
  }

  private handleLlmError(chatId: string, progressId: string, err: unknown): Observable<never> {
    const errMsg = this.sanitizeErrorMessage(err);
    this.logger.error(`❌ LLM run failed: ${errMsg}`);

    const errorText = `⚠️ ${errMsg}`;
    return this.editProgressMessage(chatId, progressId, errorText).pipe(
      catchError((editErr) => {
        this.logger.warn(
          `Failed to update error text in chat: ${this.sanitizeErrorMessage(editErr)}. Deleting progress message.`,
        );
        return this.deleteProgressMessage(chatId, progressId);
      }),
      switchMap(() => EMPTY),
    );
  }

  private formatAction(action: LlmAction): string {
    const total = action.callIds.size;
    const pending = action.pendingIds.size;
    const completed = total - pending;

    let badge = "";
    if (action.status === "error") {
      if (total > 1) {
        badge = completed > 0 ? `⚠️ [${completed}/${total}]` : `❌ [${completed}/${total}]`;
      } else {
        badge = "❌ [Error]";
      }
    } else if (action.status === "done") {
      badge = total > 1 ? `✅ [${total}/${total}]` : "✅ [Done]";
    } else {
      badge = total > 1 ? `⏳ [${completed}/${total}]` : "⏳ [Pending]";
    }

    return `${badge} ${action.comment}`.trim();
  }

  private processLlmRun(
    chatId: string,
    history: ChatMessage[],
    progressId: string,
    abort: AbortSignal,
    triggerId: string,
  ): Observable<unknown> {
    return this.llm
      .run(
        {
          spaceId: this.spaceId,
          chatId,
          botId: this.botMemberId,
          abort,
          triggerId,
        },
        history,
      )
      .pipe(
        this.requireLlmResponse(),
        concatMap((event) => {
          switch (event.type) {
            case "actions": {
              const lines = event.data.map((act) => this.formatAction(act));
              const progressText = lines.length > 0 ? lines.join("\n") : INITIAL_PROGRESS_TEXT;
              return this.editProgressMessage(chatId, progressId, progressText).pipe(
                catchError(() => EMPTY),
              );
            }

            case "response": {
              const trimmed = event.data.text.trim();
              if (!trimmed) throw new LlmEmptyResponseError();

              return this.request$(() =>
                this.anytype.addChatMessage(this.spaceId, chatId, {
                  text: trimmed,
                  reply_to_message_id: triggerId,
                }),
              ).pipe(concatMap(() => this.deleteProgressMessage(chatId, progressId)));
            }
          }
        }),
      );
  }

  private requireLlmResponse(): MonoTypeOperatorFunction<LlmEvent> {
    return connect((shared$) =>
      merge(
        shared$,
        shared$.pipe(
          filter((e) => e.type === "response"),
          throwIfEmpty(() => new LlmEmptyResponseError()),
          ignoreElements(),
        ),
      ),
    );
  }

  private request$<T>(
    factory: () => Promise<T>,
    options?: { timeoutMs?: number; retryCount?: number; retryDelayMs?: number },
  ): Observable<T> {
    const timeoutMs = options?.timeoutMs ?? SAFE_HTTP_TIMEOUT_MS;
    const retryCount = options?.retryCount ?? 0;

    return defer(factory).pipe(
      timeout(timeoutMs),
      retryCount > 0
        ? retry({ count: retryCount, delay: options?.retryDelayMs ?? PROGRESS_RETRY_DELAY_MS })
        : (source$) => source$,
    );
  }

  private progressBody(text: string) {
    return {
      text,
      marks: [{ type: "italic", from: 0, to: text.length }],
    };
  }

  private createProgressMessage(chatId: string, replyToMessageId?: string): Observable<string> {
    return this.request$(
      () =>
        this.anytype.addChatMessage(this.spaceId, chatId, {
          ...this.progressBody(INITIAL_PROGRESS_TEXT),
          reply_to_message_id: replyToMessageId,
        }),
      { timeoutMs: PROGRESS_POST_TIMEOUT_MS, retryCount: 1, retryDelayMs: PROGRESS_RETRY_DELAY_MS },
    ).pipe(map((res) => res.message_id));
  }

  private editProgressMessage(
    chatId: string,
    messageId: string,
    text: string,
  ): Observable<unknown> {
    this.logger.debug(`✏️ Progress update [${messageId}]: ${text.slice(0, 80).replace(/\n/g, " ")}`);
    return this.request$(() =>
      this.anytype.editChatMessage(this.spaceId, chatId, messageId, this.progressBody(text)),
    );
  }

  private deleteProgressMessage(chatId: string, messageId: string): Observable<void> {
    return this.request$(() =>
      this.anytype.deleteChatMessage(this.spaceId, chatId, messageId),
    ).pipe(
      map(() => void 0),
      catchError(() => EMPTY),
    );
  }

  private classifyMessage(chatId: string, msg: ChatMessageAdded): Observable<ChatIntent> {
    // 1. Message from the bot itself (cancels target trigger if replying to it)
    if (this.isBotId(msg.creator)) {
      this.logger.debug(`Message from the bot itself: ${this.preview(msg.content.text)}`);
      return of({ type: "bot_reply", replyToId: msg.reply_to_message_id });
    }

    // 2. Direct mention of the bot
    if (this.mentionsBot(msg)) {
      this.logger.debug(
        `Mention in chat ${chatId}: msg=${msg.id}: "${this.preview(msg.content?.text, 50)}"`,
      );
      return of({ type: "trigger", msg });
    }

    // 3. Reply to another message: check if replied message was from the bot
    if (msg.reply_to_message_id) {
      const replyToId = msg.reply_to_message_id;
      return this.request$(() => this.anytype.getChatMessage(this.spaceId, chatId, replyToId)).pipe(
        map((replied) => {
          if (this.isBotId(replied?.creator)) {
            this.logger.debug(
              `Reply-to-bot in chat ${chatId}: msg=${msg.id} replies to ${replyToId}`,
            );
            return { type: "trigger", msg } as const;
          }
          return { type: "noop" } as const;
        }),
        catchError(() => of({ type: "noop" } as const)),
      );
    }

    // 4. Regular message / noise
    return of({ type: "noop" });
  }

  private readonly isBotId = (wire: string | undefined): boolean =>
    Boolean(wire?.endsWith(`_${this.botMemberId}`));

  private readonly mentionsBot = (msg: ChatMessageAdded): boolean =>
    Boolean(msg.content.marks?.some((m) => m.type === "mention" && this.isBotId(m.param)));

  private readonly preview = (text: string | undefined, max = 80): string => {
    const first = text?.split("\n")[0] ?? "";
    return first.length > max ? `${first.slice(0, max)}…` : first;
  };

  private readonly sanitizeErrorMessage = (err: unknown): string => {
    const raw = err instanceof Error ? err.message : String(err);
    return raw.split("\n")[0]?.slice(0, 200) ?? "";
  };
}
