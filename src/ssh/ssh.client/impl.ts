import { Logger } from "@nestjs/common";
import type { Client, ClientChannel } from "ssh2";
import { type ExecOptions, ExecTimeoutError, SshClient, SshProcess } from ".";

/** POSIX single-quote escaping: the only safe way to splice a value into a remote shell command. */
const quote = (value: string) => `'${value.replaceAll("'", "'\\''")}'`;

export class SshProcessImpl extends SshProcess {
  private isDone = false;
  private sawExit = false;
  private exitCode: number | null = null;
  private exitSignal?: string;
  private exitError?: Error;
  private abortSignal?: AbortSignal;

  constructor(
    private readonly channel: ClientChannel,
    private readonly commandLine: string,
    private readonly opts: ExecOptions = {},
  ) {
    super();
    this.initStreams();
    this.initAbort();
  }

  private initStreams(): void {
    this.channel.setEncoding("utf8");
    this.channel.stderr.setEncoding("utf8");

    this.channel.on("data", (chunk: string) => this.stdout.next(chunk));
    this.channel.stderr.on("data", (chunk: string) => this.stderr.next(chunk));

    this.channel.once("exit", (code: number | null, signal?: string) => {
      this.sawExit = true;
      this.exitCode = code;
      this.exitSignal = code === null ? signal : undefined;
    });

    this.channel.on("error", (err: Error) => {
      this.exitError = err;
      this.kill("KILL");
    });

    this.channel.once("close", () => {
      this.isDone = true;
      this.cleanup();

      this.stdout.complete();
      this.stderr.complete();

      if (!this.sawExit && !this.exitError) {
        this.logger.warn(`⚠️ Process closed without exit status: ${this.commandLine}`);
        this.exitError = new Error(`Process closed without exit status: ${this.commandLine}`);
      }

      this._exit.resolve({
        code: this.exitCode,
        signal: this.exitSignal,
        error: this.exitError,
      });
    });

    // Send stdin or close immediately
    this.channel.end(this.opts.stdin);
  }

  private initAbort(): void {
    const signals: AbortSignal[] = [];
    if (this.opts.signal) signals.push(this.opts.signal);
    if (this.opts.timeoutMs) signals.push(AbortSignal.timeout(this.opts.timeoutMs));

    const [first, ...rest] = signals;
    if (!first) return;

    const signal = rest.length ? AbortSignal.any(signals) : first;
    this.abortSignal = signal;
    signal.addEventListener("abort", this.onAbort, { once: true });
  }

  private onAbort = (): void => {
    const isTimeout = this.abortSignal?.reason?.name === "TimeoutError";
    this.exitError = isTimeout
      ? new ExecTimeoutError(this.opts.timeoutMs ?? 0, this.commandLine)
      : new Error(`Aborted: ${this.commandLine}`);

    this.logger.warn(`🛑 Process ${isTimeout ? "timed out" : "aborted"}: ${this.commandLine}`);
    this.kill("KILL");
  };

  kill(signal: string = "KILL"): void {
    if (this.isDone) return;
    this.isDone = true;
    this.cleanup();
    try {
      this.channel.signal(signal);
    } catch {}
    try {
      this.channel.close();
    } catch {}
  }

  private cleanup(): void {
    if (this.abortSignal) {
      this.abortSignal.removeEventListener("abort", this.onAbort);
    }
  }
}

export class SshClientImpl extends SshClient {
  protected readonly logger = new Logger(this.constructor.name);

  constructor(private readonly client: Client) {
    super();
    this.client.once("close", () => this.close());
    this.client.on("error", (err) => this.logger.error(`SSH client error: ${err.message}`));
  }

  async spawn(command: string, args: string[] = [], opts: ExecOptions = {}): Promise<SshProcess> {
    if (opts.signal?.aborted) {
      throw new Error(`Execution aborted before start: ${command}`);
    }

    const line = [command, ...args].map(quote).join(" ");
    this.logger.debug(`▶️ spawn: ${command} (${args.length} arg(s))`);

    const channel = await new Promise<ClientChannel>((resolve, reject) => {
      this.client.exec(line, { env: opts.env }, (err, ch) => {
        if (err) return reject(err);
        resolve(ch);
      });
    });

    return new SshProcessImpl(channel, line, opts);
  }

  protected override onClose(): void {
    this.client.end();
  }
}
