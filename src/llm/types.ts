import { Logger } from "@nestjs/common";
import type { ModelMessage } from "ai";
import type { Observable } from "rxjs";

export abstract class LlmProvider {
  protected readonly logger = new Logger(this.constructor.name);

  abstract mode: "cli" | "api";
  abstract process: unknown;

  abstract exec(ctx: LlmRunContext, messages: ModelMessage[]): Observable<ModelMessage>;
}

export interface LlmRunContext {
  spaceId: string;
  chatId: string;
  botId: string;
  triggerId: string;
  abort: AbortSignal;
}

export type LlmActionStatus = "pending" | "done" | "error";

export class LlmAction {
  comment = "";
  callIds = new Set<string>();
  pendingIds = new Set<string>();
  error?: string;

  get status(): LlmActionStatus {
    if (this.pendingIds.size > 0) return "pending";
    if (this.error) return "error";
    return "done";
  }

  static create(message: ModelMessage | string): LlmAction {
    const action = new LlmAction();
    if (typeof message === "string") {
      action.comment = message;
      return action;
    }

    if (message.role !== "assistant" || !Array.isArray(message.content)) {
      return action;
    }

    action.callIds = LlmAction.extractCallIds(message.content);
    action.pendingIds = new Set(action.callIds);
    action.comment = LlmAction.extractComment(message.content);

    return action;
  }

  update(message: ModelMessage): LlmAction | null {
    if (message.role !== "tool" || !Array.isArray(message.content)) return null;

    let modified = false;
    let errorMessage = this.error;
    const nextPendingIds = new Set(this.pendingIds);

    for (const part of message.content) {
      if (part.type !== "tool-result") continue;
      if (!nextPendingIds.has(part.toolCallId)) continue;

      modified = true;
      nextPendingIds.delete(part.toolCallId);

      if (part.output?.type === "error-text") {
        errorMessage = part.output.value;
      } else if (part.output?.type === "execution-denied") {
        errorMessage = part.output.reason ?? "execution denied";
      }
    }

    if (!modified) return null;

    const nextAction = new LlmAction();
    nextAction.comment = this.comment;
    nextAction.callIds = new Set(this.callIds);
    nextAction.pendingIds = nextPendingIds;
    nextAction.error = errorMessage;

    return nextAction;
  }

  private static extractCallIds(parts: unknown[]): Set<string> {
    const ids = new Set<string>();

    for (const part of parts) {
      if (!part || typeof part !== "object") continue;
      if (!("type" in part) || part.type !== "tool-call") continue;
      if (!("toolCallId" in part) || typeof part.toolCallId !== "string") continue;

      ids.add(part.toolCallId);
    }

    return ids;
  }

  private static extractComment(parts: unknown[]): string {
    const toolCalls: { toolName?: string }[] = [];

    for (const part of parts) {
      if (!part || typeof part !== "object" || !("type" in part)) continue;

      if (part.type === "text" && "text" in part && typeof part.text === "string") {
        const trimmed = part.text.trim();
        if (trimmed) return trimmed;
      }

      if (part.type === "tool-call") {
        toolCalls.push(part as { toolName?: string });
      }
    }

    if (toolCalls.length > 0) {
      return toolCalls.map((c) => c.toolName || "tool").join(", ");
    }

    return "";
  }
}

export class LlmResponse {
  text = "";

  static create(text: string): LlmResponse {
    return Object.assign(new LlmResponse(), { text });
  }
}

export type LlmEvent =
  | { type: "actions"; data: LlmAction[] }
  | { type: "response"; data: LlmResponse };

export class LlmEmptyResponseError extends Error {
  constructor(message = "Empty response from LLM") {
    super(message);
    this.name = "LlmEmptyResponseError";
  }
}
