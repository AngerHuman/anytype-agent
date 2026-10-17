import type { ModelMessage } from "ai";
import { isString } from "lodash";
import { defer, finalize, type Observable, tap } from "rxjs";
import type { AppConfig } from "../../config/config.schema";
import { LlmProvider, type LlmRunContext } from "../types";

export interface ApiAliasHandler {
  issueSpaceAlias: (spaceId: string) => string;
  revokeSpaceAlias: (spaceAlias: string) => void;
}

export interface ApiParams {
  docs: string;
  port: number;
}

export interface CliParams {
  binaryPath: string;
  model?: string;
}

export abstract class LlmCliProvider extends LlmProvider {
  public readonly mode: "cli" = "cli";

  protected static readonly MAX_HISTORY_ENTRIES = 100;
  protected readonly history = new Map<string, ModelMessage[]>();

  protected abstract execInternal(
    messages: ModelMessage[],
    abort?: AbortSignal,
  ): Observable<ModelMessage>;

  protected extendSystemMessage(prompt: string): string {
    return prompt;
  }

  protected readonly systemMessageRows: string[] = [];

  constructor(
    protected readonly config: AppConfig,
    protected apiAliasHandler: ApiAliasHandler,
    protected apiParams: ApiParams,
  ) {
    super();

    this.systemMessageRows.push(
      `You are the AI assistant "${config.ANYTYPE.BOT_NAME}" in Anytype.`,
      `Instructions for interacting with Anytype:`,
      `- To read or modify data, use the local REST API proxy: http://localhost:${apiParams.port}`,
      `- You have access to the following APIs: ${apiParams.docs}`,
      `- Authorization: Already configured in the proxy server; Bearer tokens and required headers are automatically injected.`,
      `- Never show the user space alias.`,
    );
  }

  exec(ctx: LlmRunContext, messages: ModelMessage[]): Observable<ModelMessage> {
    return defer(() => {
      const spaceAlias = this.apiAliasHandler.issueSpaceAlias(ctx.spaceId);
      const systemMessageRows = [...this.systemMessageRows, `Current space alias: ${spaceAlias}`];
      const systemMessageContent = this.extendSystemMessage(systemMessageRows.join("\n"));

      const systemMessage: ModelMessage = {
        role: "system",
        content: systemMessageContent,
      };

      const historyKey = this.buildHistoryKey(ctx);
      const toolsHistory: ModelMessage[] = [];

      const enrichedMessages = this.enrichHistoryWithToolCalls(ctx, messages);

      return this.execInternal([systemMessage, ...enrichedMessages], ctx.abort).pipe(
        tap({
          next: (response) => {
            const isToolResult = response.role === "tool";
            const isToolCall = response.role === "assistant" && Array.isArray(response.content);

            if (isToolResult || isToolCall) toolsHistory.push(response);
          },
          complete: () => {
            if (!toolsHistory.length) return;

            this.saveHistoryEntry(historyKey, toolsHistory);
          },
        }),
        finalize(() => {
          this.apiAliasHandler.revokeSpaceAlias(spaceAlias);
        }),
      );
    });
  }

  protected saveHistoryEntry(key: string, messages: ModelMessage[]): void {
    if (this.history.has(key)) {
      this.history.delete(key);
    } else if (this.history.size >= LlmCliProvider.MAX_HISTORY_ENTRIES) {
      const oldestKey = this.history.keys().next().value;
      if (oldestKey) this.history.delete(oldestKey);
    }
    this.history.set(key, messages);
  }

  protected abstract readonly cliName: string;

  protected safeParseLine<T = unknown>(line: string): T | null {
    try {
      return JSON.parse(line) as T;
    } catch {
      this.logger.debug(`Non-JSON stdout: ${line}`);
      return null;
    }
  }

  protected logEmittedMessage(msg: ModelMessage): void {
    if (!Array.isArray(msg.content)) return;

    for (const part of msg.content) {
      if (part.type === "tool-call") {
        const argsPreview = JSON.stringify(part.input).slice(0, 120);
        this.logger.log(`🔧 ${this.cliName} tool call: ${part.toolName}(${argsPreview})`);
      } else if (part.type === "tool-result") {
        const isErr = part.output.type === "error-text";
        const stateLabel = isErr ? "ERROR" : "DONE";
        const outPreview =
          part.output.type === "error-text"
            ? part.output.value
            : JSON.stringify(part.output).slice(0, 120);
        this.logger.log(
          `⚙️ ${this.cliName} tool result [${stateLabel}]: ${part.toolName} -> ${outPreview}`,
        );
      }
    }
  }

  protected serializeMessagesToPrompt(messages: ModelMessage[]): string {
    if (messages.length === 0) return "";
    if (messages.length === 1 && typeof messages[0]?.content === "string") {
      return messages[0].content;
    }

    return messages
      .map((msg) => {
        let contentStr = "";
        if (typeof msg.content === "string") {
          contentStr = msg.content;
        } else if (Array.isArray(msg.content)) {
          contentStr = msg.content
            .map((part) => {
              if (part.type === "text") return part.text;
              if (part.type === "tool-call") {
                return `[Tool Call: ${part.toolName} (ID: ${part.toolCallId})] args: ${JSON.stringify(part.input)}`;
              }
              if (part.type === "tool-result") {
                let isError = false;
                let outVal = "";
                switch (part.output.type) {
                  case "text":
                    outVal = part.output.value;
                    break;
                  case "error-text":
                    isError = true;
                    outVal = part.output.value;
                    break;
                  case "json":
                    outVal = JSON.stringify(part.output.value);
                    break;
                  case "execution-denied":
                    isError = true;
                    outVal = `Execution denied: ${part.output.reason ?? "permission rejected"}`;
                    break;
                }
                return `[Tool Result: ${part.toolName} (ID: ${part.toolCallId})] ${isError ? "ERROR: " : ""}${outVal}`;
              }
              if (part.type === "reasoning") {
                return `[Reasoning]: ${part.text}`;
              }
              return "";
            })
            .filter(Boolean)
            .join("\n");
        }
        return `[${msg.role.toUpperCase()}]:\n${contentStr}`;
      })
      .join("\n\n");
  }

  protected buildHistoryKey(ctx: LlmRunContext, targetId?: string): string {
    return `${ctx.spaceId}-${ctx.chatId}-${ctx.botId}-${targetId ?? ctx.triggerId}`;
  }

  protected enrichHistoryWithToolCalls(
    ctx: LlmRunContext,
    history: ModelMessage[],
  ): ModelMessage[] {
    return history.flatMap((message) => {
      const messageId = message.providerOptions?.message?.id;
      if (!isString(messageId)) return message;

      if (message.role !== "user") return message;

      const toolCalls = this.history.get(this.buildHistoryKey(ctx, messageId));
      if (!toolCalls) return message;

      return [message, ...toolCalls];
    });
  }
}
