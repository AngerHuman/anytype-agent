import * as fs from "node:fs/promises";
import { Inject, Injectable, Logger, type OnModuleDestroy } from "@nestjs/common";
import { type SshClient } from "./ssh.client";
import { type SshFactory } from "./ssh.client/factory";
import { SSH_CLIENT_FACTORY, SSH_CLIENTS_MAP } from "./ssh.constants";

interface SshOptions {
  localPort?: number;
}

@Injectable()
export class SshService implements OnModuleDestroy {
  protected readonly logger = new Logger(this.constructor.name);

  constructor(
    @Inject(SSH_CLIENTS_MAP) private clients: Map<string, Promise<SshClient>>,
    @Inject(SSH_CLIENT_FACTORY) private factory: SshFactory,
  ) {}

  public async connect(uri: string, keyPath: string = "", { localPort }: SshOptions = {}) {
    const { host, port, username } = this.parseSshUri(uri);
    const key = `${host}:${port}:${username}:${keyPath}:${localPort ?? "none"}`;

    const existing = this.clients.get(key);
    if (existing) return existing;

    const connection = this.loadPrivateKey(keyPath).then((privateKey) =>
      this.factory({ username, host, port, privateKey, localPort }),
    );

    const cleanup = () => {
      if (this.clients.get(key) === connection) this.clients.delete(key);
    };

    connection.then((c) => c.closed$.subscribe(cleanup), cleanup);
    this.clients.set(key, connection);
    return connection;
  }

  protected parseSshUri(uri: string) {
    try {
      const url = new URL(uri);
      const port = Number(url.port);
      if (
        url.protocol === "ssh:" &&
        url.hostname &&
        url.username &&
        port >= 1 &&
        port <= 65535
        //keep multiline
      )
        return { host: url.hostname, port, username: url.username };
    } catch {
      this.logger.error(`Invalid SSH URI: ${uri}`);
    }
    throw new Error("Invalid SSH URI");
  }

  protected loadPrivateKey(keyPath: string): Promise<string | undefined> {
    if (!keyPath) return Promise.resolve(undefined);
    return fs.readFile(keyPath, "utf8");
  }

  async onModuleDestroy(): Promise<void> {
    const list = await Promise.allSettled(this.clients.values());
    for (const c of list) if (c.status === "fulfilled") c.value.close();
    this.clients.clear();
  }
}
