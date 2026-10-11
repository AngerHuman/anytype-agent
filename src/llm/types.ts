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

export class LlmAction {
  type = "ACT";
  detail = "";

  static create(detail: string): LlmAction {
    return Object.assign(new LlmAction(), { detail });
  }
}

export class LlmResponse {
  type = "RES";
  text = "";

  static create(text: string): LlmResponse {
    return Object.assign(new LlmResponse(), { text });
  }
}

export type LlmEvent = LlmAction | LlmResponse;

export class LlmEmptyResponseError extends Error {
  constructor(message = "Empty response from LLM") {
    super(message);
    this.name = "LlmEmptyResponseError";
  }
}
