import { type MCPClient } from "@ai-sdk/mcp";
import { createOpenAICompatible } from "@ai-sdk/openai-compatible";
import {
  isLoopFinished,
  type JSONValue,
  type LanguageModel,
  type ModelMessage,
  streamText,
  type TextStreamPart,
  type ToolSet,
} from "ai";
import {
  defer,
  filter,
  finalize,
  from,
  ignoreElements,
  map,
  merge,
  type Observable,
  sample,
  scan,
  share,
  startWith,
  switchMap,
  tap,
  toArray,
} from "rxjs";
import type { LlmApi as ApiConfig, AppConfig } from "../../config/config.schema";
import { LlmProvider, type LlmRunContext } from "../types";

export class LlmApiProvider extends LlmProvider {
  public readonly mode = "api";

  protected readonly systemMessageRows: string[] = [];

  protected _tools?: ReturnType<MCPClient["tools"]>;

  constructor(
    protected readonly mcp: MCPClient,
    protected readonly config: AppConfig,
    protected readonly apiConfig: ApiConfig,
    protected readonly model: LanguageModel = LlmApiProvider.getModel(apiConfig),
  ) {
    super();

    this.systemMessageRows.push(
      `You are the AI assistant "${config.ANYTYPE.BOT_NAME}" in Anytype.`,
    );
  }

  private getTools() {
    if (!this._tools) this._tools = this.mcp.tools();

    return this._tools;
  }

  exec(ctx: LlmRunContext, messages: ModelMessage[]): Observable<ModelMessage> {
    return defer(() => {
      const historyKey = this.buildHistoryKey(ctx);
      const typed$ = this.getTypedStream(ctx, messages);
      const finalText$ = this.getFinalText(typed$);

      const toolCall$ = typed$.pipe(
        filter((part) => part.type === "tool-call"),
        map(
          (part): ModelMessage => ({
            role: "assistant",
            content: [
              {
                type: "tool-call",
                toolCallId: part.toolCallId,
                toolName: part.toolName,
                input: part.input,
              },
            ],
          }),
        ),
      );

      const toolResult$ = typed$.pipe(
        filter((part) => part.type === "tool-result"),
        map(
          (part): ModelMessage => ({
            role: "tool",
            content: [
              {
                type: "tool-result",
                toolCallId: part.toolCallId,
                toolName: part.toolName,
                output: {
                  type: "json",
                  value: (part.output ?? null) as JSONValue,
                },
              },
            ],
          }),
        ),
      );

      const tools$ = merge(toolCall$, toolResult$);

      const history$ = tools$.pipe(
        toArray(),
        filter((history) => !!history.length),
        tap((history) => this.saveHistoryEntry(historyKey, history)),
        ignoreElements(),
      );

      return merge(tools$, finalText$, history$);
    });
  }

  private getTypedStream(
    ctx: LlmRunContext,
    messages: ModelMessage[],
  ): Observable<TextStreamPart<ToolSet>> {
    const sessionId = `${ctx.spaceId}:${ctx.chatId}`;

    return from(this.getTools()).pipe(
      switchMap((tools) => {
        const localAbort = new AbortController();
        const signal = ctx.abort.aborted
          ? ctx.abort
          : AbortSignal.any([ctx.abort, localAbort.signal]);

        const result = streamText({
          model: this.model,
          system: this.systemMessageRows.join("\n"),
          messages: this.enrichHistoryWithToolCalls(ctx, messages),
          tools,
          stopWhen: isLoopFinished(),
          abortSignal: signal,
          headers: {
            // TODO: think about more generic approach
            "x-opencode-session": sessionId,
          },
        });

        return from(result.stream).pipe(
          filter((part) => !!part && "type" in part),
          tap((part) => {
            if (part.type !== "error") return;

            const err = part.error;
            if (err instanceof Error) throw err;
            if (err && typeof err === "object") {
              const msg =
                "message" in err && typeof err.message === "string"
                  ? err.message
                  : JSON.stringify(err);
              throw new Error(msg);
            }
            throw new Error(String(err));
          }),
          finalize(() => {
            localAbort.abort();
          }),
        );
      }),
      share(),
    );
  }

  private getFinalText(events$: ReturnType<typeof this.getTypedStream>): Observable<ModelMessage> {
    const text$ = events$.pipe(
      filter((part) => part.type === "text-delta"),
      scan((acc, curr) => acc + curr.text, ""),
      startWith(""),
    );

    const finish$ = events$.pipe(filter((part) => part.type === "finish"));

    return text$.pipe(
      sample(finish$),
      map((text) => text.trim()),
      filter((text) => text.length > 0),
      map((content): ModelMessage => ({ role: "assistant", content })),
    );
  }

  private static getModel(apiConfig: ApiConfig): LanguageModel {
    const provider = createOpenAICompatible({
      name: apiConfig.NAME || "opencode",
      baseURL: apiConfig.API_URL,
      apiKey: apiConfig.API_KEY,
    });

    return provider.chatModel(apiConfig.MODEL);
  }
}
