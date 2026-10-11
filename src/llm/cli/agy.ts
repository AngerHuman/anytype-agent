import type { JSONValue, ModelMessage } from "ai";
import {
  concat,
  defer,
  filter,
  from,
  map,
  mergeMap,
  Observable,
  of,
  scan,
  switchMap,
  tap,
} from "rxjs";
import type { AppConfig } from "../../config/config.schema";
import type { SshClient } from "../../ssh/ssh.client";
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

interface AgyStepUpdateEvent {
  event: "step_update";
  step_update: {
    conversation_id?: string;
    step_index: number;
    state: "ACTIVE" | "DONE" | "ERROR";
    step_type: "user_input" | "agent_response" | "tool";
    text_delta?: string;
    tool_name?: string;
    tool_info?: {
      name?: string;
      parameters?: Record<string, unknown>;
      output?: unknown;
      error?: { type: string; message: string } | string;
    };
    duration_seconds?: number;
    usage?: Record<string, unknown>;
  };
}

interface AgyResultEvent {
  event: "result";
  result: {
    conversation_id?: string;
    status: "SUCCESS" | "FAILURE";
    response?: string;
    error?: string;
  };
}

type AgyEvent = AgyInitEvent | AgyStepUpdateEvent | AgyResultEvent;

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
        switchMap(({ stdout$ }) => this.splitLines(stdout$)),
        map((line) => this.safeParseLine(line)),
        filter((event): event is AgyEvent => event !== null),
        mergeMap((event) => this.mapAgyEvent(event)),
        tap({ unsubscribe: () => local.abort() }),
      );
    });
  }

  override extendSystemMessage(msg: string) {
    return `${msg}

    Communication & Language Guidelines:
    - ALWAYS respond in the exact same language used by the user in their message or query (e.g., if the user wrote in Russian, answer in Russian; if in English, answer in English).
    - Formulate a helpful, structured, and clear response.
    - Never show the user SPACE ID;
    - Do not use Markdown in your responses.
    `;
  }

  private splitLines(stdout$: Observable<string>): Observable<string> {
    return concat(stdout$, of("\n")).pipe(
      scan(
        ({ buffer }, chunk) => {
          const combined = buffer + chunk;
          const lines = combined.split("\n");
          const rest = lines.pop() ?? "";
          return { buffer: rest, lines };
        },
        { buffer: "", lines: [] as string[] },
      ),
      mergeMap(({ lines }) => lines),
      map((line) => line.trim()),
      filter(Boolean),
    );
  }

  private mapAgyEvent(event: AgyEvent): ModelMessage[] {
    if (event.event === "step_update") {
      const step = event.step_update;

      if (step.step_type === "tool" && step.state === "ACTIVE") {
        const toolName = step.tool_name || step.tool_info?.name || "unknown_tool";
        const input = (step.tool_info?.parameters ?? {}) as Record<string, unknown>;
        this.logger.log(`🔧 agy tool call: ${toolName}(${JSON.stringify(input).slice(0, 120)})`);

        return [
          {
            role: "assistant",
            content: [
              {
                type: "tool-call",
                toolCallId: `call_${step.step_index}`,
                toolName,
                input,
              },
            ],
          },
        ];
      }

      if (step.step_type === "tool" && (step.state === "DONE" || step.state === "ERROR")) {
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

        this.logger.log(
          `⚙️ agy tool result [${step.state}]: ${toolName} -> ${
            output.type === "error-text" ? output.value : JSON.stringify(output).slice(0, 120)
          }`,
        );

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
    }

    if (event.event === "result") {
      this.logger.log(
        `🏁 agy final result [${event.result.status}]: "${(event.result.response || "").slice(
          0,
          100,
        )}"${event.result.error ? ` error: ${event.result.error}` : ""}`,
      );
      if (event.result.status === "FAILURE") {
        throw new Error(`agy execution failed: ${event.result.error || "unknown"}`);
      }

      return [
        {
          role: "assistant",
          content: event.result.response ?? "",
        },
      ];
    }

    return [];
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
