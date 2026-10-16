import { Inject, Injectable } from "@nestjs/common";
import type { ModelMessage } from "ai";
import isString from "lodash/isString";
import { defer, EMPTY, filter, map, type Observable } from "rxjs";
import type { ChatMessage } from "../client";
import { LLM_PROVIDERS } from "./llm.constants";
import { LlmAction, type LlmEvent, LlmProvider, LlmResponse, type LlmRunContext } from "./types";

class LlmStreamAggregator {
  private actions: LlmAction[] = [];

  handleMessage(message: ModelMessage): LlmEvent | null {
    if (message.role === "assistant") {
      if (Array.isArray(message.content)) {
        const hasToolCalls = message.content.some((p) => p.type === "tool-call");

        if (hasToolCalls) {
          this.actions = [...this.actions, LlmAction.create(message)];
          return { type: "actions", data: this.actions };
        }

        const textPart = message.content.find((p) => p.type === "text");
        if (
          textPart &&
          "text" in textPart &&
          typeof textPart.text === "string" &&
          textPart.text.trim()
        ) {
          return { type: "response", data: LlmResponse.create(textPart.text) };
        }
      }

      if (isString(message.content)) {
        return { type: "response", data: LlmResponse.create(message.content) };
      }
    }

    if (message.role === "tool") {
      let updated = false;
      const nextActions = this.actions.map((act) => {
        const next = act.update(message);
        if (next) {
          updated = true;
          return next;
        }
        return act;
      });

      if (updated) {
        this.actions = nextActions;
        return { type: "actions", data: this.actions };
      }
    }

    return null;
  }
}

@Injectable()
export class LlmService {
  constructor(@Inject(LLM_PROVIDERS) private readonly providers: LlmProvider[]) {}

  run(ctx: LlmRunContext, messages: ChatMessage[]): Observable<LlmEvent> {
    return defer(() => {
      if (ctx.abort.aborted) return EMPTY;

      // 1. Готовим чистые диалоговые сообщения
      const history = this.mapChatMessages(messages, ctx.botId);

      // 2. Берем провайдер (в будущем тут фоллбек через catchError)
      const targetProvider = this.providers[0];
      if (!targetProvider) throw new Error("No LLM provider available");

      // 3. Вызываем провайдер: передаем контекст и чистую историю
      const aggregator = new LlmStreamAggregator();
      return targetProvider.exec(ctx, history).pipe(
        map((message) => aggregator.handleMessage(message)),
        filter((event): event is LlmEvent => event !== null),
      );
    });
  }

  private mapChatMessages(messages: ChatMessage[], botId: string): ModelMessage[] {
    return messages.map((msg): ModelMessage => {
      const isBot = this.isBotId(botId, msg.creator);
      const content = msg.content?.text ?? "";
      const providerOptions = { message: { id: msg.id } };

      return isBot
        ? { role: "assistant", content, providerOptions }
        : { role: "user", content, providerOptions };
    });
  }

  private isBotId(botId: string, wire?: string): boolean {
    return Boolean(wire?.endsWith(`_${botId}`));
  }
}
