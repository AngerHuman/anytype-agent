import { Logger } from "@nestjs/common";
import { Observable, Subject } from "rxjs";

export interface ExecExitResult {
  code: number | null;
  signal?: string;
  error?: Error;
}

export interface ExecOptions {
  stdin?: string | Uint8Array;
  timeoutMs?: number;
  env?: Record<string, string>;
  signal?: AbortSignal;
}

// TODO add global interface of SpawnedProcess
// {stdout$, stderr$, exit, kill}
export abstract class SshProcess {
  protected readonly logger = new Logger(this.constructor.name);

  protected readonly stdout = new Subject<string>();
  protected readonly stderr = new Subject<string>();
  protected readonly _exit = Promise.withResolvers<ExecExitResult>();

  readonly stdout$: Observable<string> = this.stdout.asObservable();
  readonly stderr$: Observable<string> = this.stderr.asObservable();
  readonly exit = this._exit.promise;

  abstract kill(signal?: string): void;
}

export class ExecTimeoutError extends Error {
  constructor(timeoutMs: number, command: string) {
    super(`Process '${command}' timed out after ${timeoutMs}ms`);
    this.name = "ExecTimeoutError";
  }
}

// TODO add global interface of ProcessSpawner
export abstract class SshClient {
  forwardPort?: { localPort: number; remotePort: number };

  private isClosed = false;
  protected readonly _closed = new Subject<void>();
  readonly closed$: Observable<void> = this._closed.asObservable();

  abstract spawn(command: string, args?: string[], opts?: ExecOptions): Promise<SshProcess>;

  close(): void {
    if (this.isClosed) return;
    this.isClosed = true;
    this._closed.next();
    this._closed.complete();
    this.onClose();
  }

  protected onClose(): void {}
}
