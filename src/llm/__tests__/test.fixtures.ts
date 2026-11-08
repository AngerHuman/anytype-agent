import { mock } from "bun:test";
import type { ModelMessage } from "ai";
import { type Observable, of, Subject } from "rxjs";
import type { AppConfig } from "../../config/config.schema";
import type { SshClient, SshProcess } from "../../ssh/ssh.client";
import type { ApiAliasHandler, ApiParams } from "../cli";
import { LlmProvider, type LlmRunContext } from "../types";

export const createTestRunContext = (overrides: Partial<LlmRunContext> = {}): LlmRunContext => ({
  spaceId: "space-1",
  chatId: "chat-1",
  triggerId: "msg-1",
  botId: "bot-1",
  abort: new AbortController().signal,
  ...overrides,
});

export const mockCliConfig = {
  ANYTYPE: {
    BOT_NAME: "TestBot",
  },
} as unknown as AppConfig;

export const mockApiParams: ApiParams = {
  port: 5000,
  docs: "GET /v1/spaces",
};

export interface MockAliasHandler extends ApiAliasHandler {
  revoked: string[];
}

export const createMockAliasHandler = (alias = "alias-123"): MockAliasHandler => {
  const revoked: string[] = [];
  return {
    issueSpaceAlias: mock((_spaceId: string) => alias),
    revokeSpaceAlias: mock((revokedAlias: string) => {
      revoked.push(revokedAlias);
    }),
    revoked,
  };
};

export function createMockProcess(stdout$: Subject<string>, onKill = mock(() => {})): SshProcess {
  return {
    stdout$: stdout$.asObservable(),
    stderr$: of(),
    exit: Promise.resolve({ code: 0 }),
    kill: onKill,
  } as unknown as SshProcess;
}

export interface MockSshSetup {
  client: SshClient;
  spawnMock: ReturnType<typeof mock>;
  getCapturedOpts: () => { stdin?: string; signal?: AbortSignal } | undefined;
}

export function createMockSshClient(
  stdout$: Subject<string>,
  onKill = mock(() => {}),
): MockSshSetup {
  let capturedOpts: { stdin?: string; signal?: AbortSignal } | undefined;
  const spawnMock = mock(async (_cmd, _args, opts) => {
    capturedOpts = opts;
    return createMockProcess(stdout$, onKill);
  });

  return {
    client: { spawn: spawnMock } as unknown as SshClient,
    spawnMock,
    getCapturedOpts: () => capturedOpts,
  };
}

export class MockLlmProvider extends LlmProvider {
  public readonly mode = "cli";
  public readonly process = null;
  public capturedCtx?: LlmRunContext;
  public capturedMessages: ModelMessage[] = [];
  public responseSubject = new Subject<ModelMessage>();

  exec(ctx: LlmRunContext, messages: ModelMessage[]): Observable<ModelMessage> {
    this.capturedCtx = ctx;
    this.capturedMessages = messages;
    return this.responseSubject.asObservable();
  }
}
