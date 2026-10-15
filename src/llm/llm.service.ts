import { Inject, Injectable } from "@nestjs/common";
import type { ModelMessage } from "ai";
import isString from "lodash/isString";
import { defer, EMPTY, filter, map, type Observable } from "rxjs";
import type { ChatMessage } from "../client";
import { LLM_PROVIDERS } from "./llm.constants";
import { LlmAction, type LlmEvent, LlmProvider, LlmResponse, type LlmRunContext } from "./types";

@Injectable()
export class LlmService {
  constructor(@Inject(LLM_PROVIDERS) private readonly providers: LlmProvider[]) {}

  //TODO: return Observable<[LlmAction[], LlmResponse | null]>
  run(ctx: LlmRunContext, messages: ChatMessage[]): Observable<LlmEvent> {
    return defer(() => {
      if (ctx.abort.aborted) return EMPTY;

      // 1. Готовим чистые диалоговые сообщения
      const history = this.mapChatMessages(messages, ctx.botId);

      // 2. Берем провайдер (в будущем тут фоллбек через catchError)
      const targetProvider = this.providers[0];
      if (!targetProvider) throw new Error("No LLM provider available");

      // 3. Вызываем провайдер: передаем контекст и чистую историю
      return targetProvider.exec(ctx, history).pipe(
        map((response) => {
          if (response.role === "assistant") {
            if (Array.isArray(response.content)) {
              const textPart = response.content.find((p) => p.type === "text");
              if (textPart?.text.trim()) return LlmAction.create(textPart.text.trim());
            }

            if (isString(response.content)) return LlmResponse.create(response.content);
          }

          if (response.role === "tool") return LlmAction.create(JSON.stringify(response.content));
        }),
        filter((event): event is LlmEvent => event !== undefined),
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
