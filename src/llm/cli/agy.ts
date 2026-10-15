import type { JSONValue, ModelMessage } from "ai";
import {
  concat,
  concatMap,
  defer,
  filter,
  from,
  map,
  Observable,
  type OperatorFunction,
  of,
  scan,
  switchMap,
  tap,
} from "rxjs";
import Type, { type Static } from "typebox";
import type { AppConfig } from "../../config/config.schema";
import type { SshClient } from "../../ssh/ssh.client";
import { check } from "../../type.registry";
import { type ApiAliasHandler, type ApiParams, LlmCliProvider } from ".";

interface AgyInitEvent {
  event: "init";
  conversation_id: string;
  init?: {
    cwd?: string;
    tools?: string[];
    permission_mode?: string;
  };
}

const AgyAgentResponse = Type.Object({
  event: Type.Literal("step_update"),
  step_update: Type.Object({
    step_index: Type.Number(),
    state: Type.Union([Type.Literal("ACTIVE"), Type.Literal("DONE")]),
    step_type: Type.Literal("agent_response"),
    text_delta: Type.Optional(Type.String()),
  }),
});
type AgyAgentResponse = Static<typeof AgyAgentResponse>;

const ToolCallInfo = Type.Object({
  name: Type.Optional(Type.String()),
  parameters: Type.Optional(Type.Record(Type.String(), Type.Any())),
});

const AgyToolCall = Type.Object({
  event: Type.Literal("step_update"),
  step_update: Type.Object({
    step_index: Type.Number(),
    state: Type.Literal("ACTIVE"),
    step_type: Type.Literal("tool"),
    tool_name: Type.Optional(Type.String()),
    tool_info: Type.Optional(ToolCallInfo),
  }),
});
type AgyToolCall = Static<typeof AgyToolCall>;

const ToolDoneInfo = Type.Object({
  name: Type.Optional(Type.String()),
  parameters: Type.Optional(Type.Record(Type.String(), Type.Any())),
  output: Type.Optional(Type.Any()),
  error: Type.Optional(
    Type.Union([
      Type.String(),
      Type.Object({
        type: Type.Optional(Type.String()),
        message: Type.String(),
      }),
    ]),
  ),
});

const AgyToolDone = Type.Object({
  event: Type.Literal("step_update"),
  step_update: Type.Object({
    step_index: Type.Number(),
    state: Type.Union([Type.Literal("DONE"), Type.Literal("ERROR")]),
    step_type: Type.Literal("tool"),
    tool_name: Type.Optional(Type.String()),
    tool_info: Type.Optional(ToolDoneInfo),
  }),
});
type AgyToolDone = Static<typeof AgyToolDone>;

const AgyResult = Type.Object({
  event: Type.Literal("result"),
  result: Type.Object({
    status: Type.Union([
      Type.Literal("SUCCESS"),
      Type.Literal("ERROR"),
      Type.Literal("FAILURE"),
      Type.String(),
    ]),
    response: Type.Optional(Type.String()),
    error: Type.Optional(Type.Any()),
  }),
});
type AgyResult = Static<typeof AgyResult>;

type AgyEvent = AgyInitEvent | AgyAgentResponse | AgyToolCall | AgyToolDone | AgyResult;

interface StreamAccumulator {
  step_index?: number;
  textBuffer: string;
  emittedMessages: ModelMessage[];
}

export function splitLines(): OperatorFunction<string, string> {
  return (source$: Observable<string>) =>
    concat(source$, of("\n")).pipe(
      scan(
        ({ buffer }, chunk) => {
          const combined = buffer + chunk;
          const lines = combined.split("\n");
          const rest = lines.pop() ?? "";
          return { buffer: rest, lines };
        },
        { buffer: "", lines: [] as string[] },
      ),
      concatMap(({ lines }) => lines),
      map((line) => line.trim()),
      filter(Boolean),
    );
}

export class AgyCliProvider extends LlmCliProvider {
  constructor(
    // TODO remove this decoupling, introduce a ProcessSpawner
    public readonly process: SshClient,
    config: AppConfig,
    apiAliasHandler: ApiAliasHandler,
    apiParams: ApiParams,
    private readonly binaryPath: string = "agy",
  ) {
    super(config, apiAliasHandler, apiParams);
  }

  protected execInternal(messages: ModelMessage[], abort?: AbortSignal): Observable<ModelMessage> {
    const args = [
      "--input-format",
      "text",
      "--output-format",
      "stream-json",
      "--dangerously-skip-permissions",
      "--disable-slash-commands",
    ];

    const stdinText = this.serializeMessagesToPrompt(messages);

    return defer(() => {
      const local = new AbortController();
      const signal = abort ? AbortSignal.any([abort, local.signal]) : local.signal;

      return from(
        this.process.spawn(this.binaryPath, args, { stdin: stdinText, timeoutMs: 300_000, signal }),
      ).pipe(
        switchMap(({ stdout$ }) => stdout$.pipe(splitLines())),
        map((line) => this.safeParseLine(line)),
        filter((event): event is AgyEvent => event !== null),
        tap((event) => this.assertNotFailed(event)),
        scan<AgyEvent, StreamAccumulator>((acc, event) => this.processEventInStream(acc, event), {
          textBuffer: "",
          emittedMessages: [],
        }),
        concatMap(({ emittedMessages }) => emittedMessages),
        tap({
          next: (msg) => this.logEmittedMessage(msg),
          unsubscribe: () => local.abort(),
        }),
      );
    });
  }

  override extendSystemMessage(msg: string) {
    return [
      msg,
      "",
      "Communication & Language Guidelines:",
      "- ALWAYS respond in the exact same language used by the user in their message or query (e.g., if the user wrote in Russian, answer in Russian; if in English, answer in English).",
      "- Formulate a helpful, structured, and clear response.",
      "- Never show the user SPACE ID;",
      "- Do not use Markdown in your responses.",
      "",
      "Tool Call Guidelines:",
      "- Before calling ANY tool, output a short single-sentence explanation of what you are about to do.",
    ].join("\n");
  }

  private processEventInStream(acc: StreamAccumulator, event: AgyEvent): StreamAccumulator {
    if (check(AgyAgentResponse, event)) {
      const delta = event.step_update.text_delta ?? "";
      if (acc.step_index !== event.step_update.step_index)
        return {
          textBuffer: delta,
          step_index: event.step_update.step_index,
          emittedMessages: [],
        };

      return {
        ...acc,
        textBuffer: acc.textBuffer + delta,
      };
    }

    if (check(AgyToolCall, event)) {
      const comment = acc.textBuffer.trim() || undefined;

      return {
        textBuffer: "",
        emittedMessages: this.mapToolCallEvent(event, comment),
      };
    }

    if (check(AgyToolDone, event)) {
      return {
        textBuffer: "",
        emittedMessages: this.mapToolResultEvent(event),
      };
    }

    if (check(AgyResult, event)) {
      const response = acc.textBuffer.trim() || (event.result.response ?? "").trim();
      return {
        textBuffer: "",
        emittedMessages: response
          ? [
              {
                role: "assistant",
                content: response,
              },
            ]
          : [],
      };
    }

    if (event.event !== "init") {
      this.logger.debug(`Unrecognized agy event: ${JSON.stringify(event)}`);
    }

    return acc.emittedMessages.length === 0 ? acc : { ...acc, emittedMessages: [] };
  }

  private mapToolCallEvent(
    { step_update: step }: AgyToolCall,
    accompanyingText?: string,
  ): ModelMessage[] {
    const toolName = step.tool_name || step.tool_info?.name || "unknown_tool";
    const input = (step.tool_info?.parameters ?? {}) as Record<string, unknown>;

    return [
      {
        role: "assistant",
        content: [
          ...(accompanyingText ? [{ type: "text" as const, text: accompanyingText }] : []),
          {
            type: "tool-call" as const,
            toolCallId: `call_${step.step_index}`,
            toolName,
            input,
          },
        ],
      },
    ];
  }

  private mapToolResultEvent({ step_update: step }: AgyToolDone): ModelMessage[] {
    const toolName = step.tool_name || step.tool_info?.name || "unknown_tool";
    let output:
      | { type: "text"; value: string }
      | { type: "error-text"; value: string }
      | { type: "json"; value: JSONValue };

    if (step.state === "ERROR" || step.tool_info?.error) {
      const err = step.tool_info?.error;
      const errMsg = typeof err === "string" ? err : (err?.message ?? "Tool execution failed");
      output = {
        type: "error-text",
        value: errMsg,
      };
    } else if (typeof step.tool_info?.output === "string") {
      output = {
        type: "text",
        value: step.tool_info.output,
      };
    } else if (step.tool_info?.output !== undefined) {
      output = {
        type: "json",
        value: step.tool_info.output as JSONValue,
      };
    } else {
      output = {
        type: "text",
        value: "",
      };
    }

    return [
      {
        role: "tool",
        content: [
          {
            type: "tool-result",
            toolCallId: `call_${step.step_index}`,
            toolName,
            output,
          },
        ],
      },
    ];
  }

  private assertNotFailed(event: AgyEvent): void {
    if (event.event === "result" && event.result.status !== "SUCCESS") {
      const errorMsg =
        typeof event.result.error === "string"
          ? event.result.error
          : JSON.stringify(event.result.error) || "unknown";
      throw new Error(`agy execution failed: ${errorMsg}`);
    }
  }

  private logEmittedMessage(msg: ModelMessage): void {
    if (!Array.isArray(msg.content)) return;

    for (const part of msg.content) {
      if (part.type === "tool-call") {
        const argsPreview = JSON.stringify(part.input).slice(0, 120);
        this.logger.log(`🔧 agy tool call: ${part.toolName}(${argsPreview})`);
      } else if (part.type === "tool-result") {
        const isErr = part.output.type === "error-text";
        const stateLabel = isErr ? "ERROR" : "DONE";
        const outPreview =
          part.output.type === "error-text"
            ? part.output.value
            : JSON.stringify(part.output).slice(0, 120);
        this.logger.log(`⚙️ agy tool result [${stateLabel}]: ${part.toolName} -> ${outPreview}`);
      }
    }
  }

  private serializeMessagesToPrompt(messages: ModelMessage[]): string {
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

  private safeParseLine(line: string): AgyEvent | null {
    try {
      return JSON.parse(line);
    } catch {
      this.logger.debug(`Non-JSON stdout: ${line}`);
      return null;
    }
  }
}
