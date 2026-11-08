import type { JSONValue, ModelMessage } from "ai";
import { concatMap, defer, filter, from, map, Observable, scan, switchMap, tap } from "rxjs";
import Type, { type Static } from "typebox";
import type { AppConfig } from "../../config/config.schema";
import type { SshClient } from "../../ssh/ssh.client";
import { check } from "../../type.registry";
import { type ApiAliasHandler, type ApiParams, type CliParams, LlmCliProvider } from "./index";
import { splitLines } from "./split-lines";

/**
 * OpenCode CLI Event Schemas (TypeBox)
 * Based on real OpenCode CLI v1.18.x (`opencode run --format json`)
 */

const BaseEvent = Type.Object({
  timestamp: Type.Number(),
  sessionID: Type.String(),
});

export const OpenCodeStepStartEvent = Type.Intersect([
  BaseEvent,
  Type.Object({
    type: Type.Literal("step_start"),
    part: Type.Object({
      id: Type.Optional(Type.String()),
      messageID: Type.Optional(Type.String()),
      sessionID: Type.Optional(Type.String()),
      type: Type.Literal("step-start"),
      snapshot: Type.Optional(Type.String()),
    }),
  }),
]);
export type OpenCodeStepStartEvent = Static<typeof OpenCodeStepStartEvent>;

export const OpenCodeTextEvent = Type.Intersect([
  BaseEvent,
  Type.Object({
    type: Type.Literal("text"),
    part: Type.Object({
      id: Type.Optional(Type.String()),
      messageID: Type.Optional(Type.String()),
      sessionID: Type.Optional(Type.String()),
      type: Type.Literal("text"),
      text: Type.String(),
      time: Type.Optional(
        Type.Object({
          start: Type.Number(),
          end: Type.Optional(Type.Number()),
        }),
      ),
    }),
  }),
]);
export type OpenCodeTextEvent = Static<typeof OpenCodeTextEvent>;

export const OpenCodeReasoningEvent = Type.Intersect([
  BaseEvent,
  Type.Object({
    type: Type.Literal("reasoning"),
    part: Type.Object({
      id: Type.Optional(Type.String()),
      messageID: Type.Optional(Type.String()),
      sessionID: Type.Optional(Type.String()),
      type: Type.Literal("reasoning"),
      text: Type.String(),
      time: Type.Optional(
        Type.Object({
          start: Type.Number(),
          end: Type.Optional(Type.Number()),
        }),
      ),
    }),
  }),
]);
export type OpenCodeReasoningEvent = Static<typeof OpenCodeReasoningEvent>;

export const OpenCodeToolUseEvent = Type.Intersect([
  BaseEvent,
  Type.Object({
    type: Type.Literal("tool_use"),
    part: Type.Object({
      id: Type.Optional(Type.String()),
      messageID: Type.Optional(Type.String()),
      sessionID: Type.Optional(Type.String()),
      type: Type.Literal("tool"),
      tool: Type.String(),
      callID: Type.String(),
      state: Type.Object({
        status: Type.Union([Type.Literal("completed"), Type.Literal("error")]),
        input: Type.Record(Type.String(), Type.Any()),
        output: Type.Optional(Type.String()),
        error: Type.Optional(Type.Any()),
        title: Type.Optional(Type.String()),
      }),
    }),
  }),
]);
export type OpenCodeToolUseEvent = Static<typeof OpenCodeToolUseEvent>;

export const OpenCodeStepFinishEvent = Type.Intersect([
  BaseEvent,
  Type.Object({
    type: Type.Literal("step_finish"),
    part: Type.Object({
      id: Type.Optional(Type.String()),
      messageID: Type.Optional(Type.String()),
      sessionID: Type.Optional(Type.String()),
      type: Type.Literal("step-finish"),
      reason: Type.String(),
      tokens: Type.Optional(Type.Record(Type.String(), Type.Any())),
      cost: Type.Optional(Type.Number()),
    }),
  }),
]);
export type OpenCodeStepFinishEvent = Static<typeof OpenCodeStepFinishEvent>;

export const OpenCodeErrorEvent = Type.Intersect([
  BaseEvent,
  Type.Object({
    type: Type.Literal("error"),
    error: Type.Object({
      name: Type.String(),
      data: Type.Optional(
        Type.Object({
          message: Type.String(),
          statusCode: Type.Optional(Type.Number()),
          isRetryable: Type.Optional(Type.Boolean()),
        }),
      ),
    }),
  }),
]);
export type OpenCodeErrorEvent = Static<typeof OpenCodeErrorEvent>;

export type OpenCodeEvent =
  | OpenCodeStepStartEvent
  | OpenCodeTextEvent
  | OpenCodeReasoningEvent
  | OpenCodeToolUseEvent
  | OpenCodeStepFinishEvent
  | OpenCodeErrorEvent;

export interface OpenCodeStreamAccumulator {
  textBuffer: string;
  emittedMessages: ModelMessage[];
}

export class OpenCodeCliProvider extends LlmCliProvider {
  protected readonly cliName = "opencode";
  private readonly binaryPath: string;
  private readonly model: string;

  constructor(
    public readonly process: SshClient,
    config: AppConfig,
    apiAliasHandler: ApiAliasHandler,
    apiParams: ApiParams,
    cliParams: CliParams,
  ) {
    super(config, apiAliasHandler, apiParams);

    if (!cliParams.model?.trim()) {
      throw new Error("OpenCodeCliProvider requires a model to be explicitly configured");
    }

    this.binaryPath = cliParams.binaryPath;
    this.model = cliParams.model.trim();
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

  protected execInternal(messages: ModelMessage[], abort?: AbortSignal): Observable<ModelMessage> {
    const args = ["run", "-m", this.model, "--format", "json", "--auto"];
    const stdinText = this.serializeMessagesToPrompt(messages);

    return defer(() => {
      const local = new AbortController();
      const signal = abort ? AbortSignal.any([abort, local.signal]) : local.signal;

      return from(
        this.process.spawn(this.binaryPath, args, { stdin: stdinText, timeoutMs: 300_000, signal }),
      ).pipe(
        switchMap(({ stdout$ }) => stdout$.pipe(splitLines())),
        map((line) => this.safeParseLine(line)),
        filter((event): event is OpenCodeEvent => event !== null),
        tap((event) => this.assertNotFailed(event)),
        scan<OpenCodeEvent, OpenCodeStreamAccumulator>(
          (acc, event) => this.processEventInStream(acc, event),
          {
            textBuffer: "",
            emittedMessages: [],
          },
        ),
        concatMap(({ emittedMessages }) => emittedMessages),
        tap({
          next: (msg) => this.logEmittedMessage(msg),
          unsubscribe: () => local.abort(),
        }),
      );
    });
  }

  protected processEventInStream(
    acc: OpenCodeStreamAccumulator,
    event: OpenCodeEvent,
  ): OpenCodeStreamAccumulator {
    if (check(OpenCodeTextEvent, event)) {
      const chunk = event.part.text;
      return {
        ...acc,
        textBuffer: acc.textBuffer + chunk,
        emittedMessages: [],
      };
    }

    if (check(OpenCodeToolUseEvent, event)) {
      const comment = acc.textBuffer.trim() || undefined;
      const toolCallMsg = this.mapToolCallEvent(event, comment);
      const toolResultMsg = this.mapToolResultEvent(event);

      return {
        ...acc,
        textBuffer: "",
        emittedMessages: [...toolCallMsg, ...toolResultMsg],
      };
    }

    if (check(OpenCodeStepFinishEvent, event)) {
      if (event.part.reason === "stop" || event.part.reason === "length") {
        const finalText = acc.textBuffer.trim();
        return {
          textBuffer: "",
          emittedMessages: finalText
            ? [
                {
                  role: "assistant",
                  content: finalText,
                },
              ]
            : [],
        };
      }
      return acc.emittedMessages.length === 0 ? acc : { ...acc, emittedMessages: [] };
    }

    if (check(OpenCodeStepStartEvent, event) || check(OpenCodeReasoningEvent, event)) {
      return acc.emittedMessages.length === 0 ? acc : { ...acc, emittedMessages: [] };
    }

    this.logger.debug(`Unrecognized opencode event: ${JSON.stringify(event)}`);
    return acc.emittedMessages.length === 0 ? acc : { ...acc, emittedMessages: [] };
  }

  protected mapToolCallEvent(
    event: OpenCodeToolUseEvent,
    accompanyingText?: string,
  ): ModelMessage[] {
    const toolName = event.part.tool;
    const input = event.part.state.input ?? {};

    return [
      {
        role: "assistant",
        content: [
          ...(accompanyingText ? [{ type: "text" as const, text: accompanyingText }] : []),
          {
            type: "tool-call" as const,
            toolCallId: event.part.callID,
            toolName,
            input,
          },
        ],
      },
    ];
  }

  protected mapToolResultEvent(event: OpenCodeToolUseEvent): ModelMessage[] {
    const toolName = event.part.tool;
    const isError = event.part.state.status === "error";

    let output:
      | { type: "text"; value: string }
      | { type: "error-text"; value: string }
      | { type: "json"; value: JSONValue };

    if (isError) {
      const err = event.part.state.error;
      const errMsg =
        typeof err === "string" ? err : (err?.message ?? JSON.stringify(err) ?? "Tool failed");
      output = {
        type: "error-text",
        value: errMsg,
      };
    } else {
      output = {
        type: "text",
        value: event.part.state.output ?? "",
      };
    }

    return [
      {
        role: "tool",
        content: [
          {
            type: "tool-result",
            toolCallId: event.part.callID,
            toolName,
            output,
          },
        ],
      },
    ];
  }

  protected assertNotFailed(event: OpenCodeEvent): void {
    if (event.type === "error") {
      const errorMsg = event.error.data?.message ?? event.error.name ?? "OpenCode execution failed";
      throw new Error(`opencode execution failed: ${errorMsg}`);
    }
  }
}
