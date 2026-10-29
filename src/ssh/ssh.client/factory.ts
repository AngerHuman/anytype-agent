import * as net from "node:net";
import { pipeline } from "node:stream";
import type { Logger } from "@nestjs/common";
import { Client } from "ssh2";
import type { SshClient } from ".";
import { SshClientImpl } from "./impl";

export class SshConnectionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SshConnectionError";
  }
}

export interface SshClientProps {
  username: string;
  host: string;
  port?: number;
  privateKey?: string | Buffer;
  localPort?: number;
}

export type SshClientFactory = (
  this: { logger: Logger },
  props: SshClientProps,
) => Promise<SshClient>;

export type SshFactory = ReturnType<typeof sshClientFactory.bind<typeof sshClientFactory>>;

function setupPortForward(
  client: Client,
  connection: SshClientImpl,
  localPort: number,
  logger: Logger,
): Promise<void> {
  const { promise, resolve, reject } = Promise.withResolvers<void>();

  client.forwardIn("127.0.0.1", 0, (err, remotePort) => {
    if (err) {
      client.end();
      return reject(new SshConnectionError(err.message));
    }
    logger.log(`Host listens on port: ${remotePort}`);
    connection.forwardPort = { localPort, remotePort };
    resolve();
  });

  client.on("tcp connection", (info, accept, rejectConnection) => {
    logger.log(`TCP connection: ${info.srcIP}:${info.srcPort} -> ${info.destPort}`);
    let accepted = false;

    const localSocket = net.connect(localPort, "127.0.0.1", () => {
      const remoteStream = accept();
      accepted = true;
      pipeline(remoteStream, localSocket, (err) => {
        if (err) logger.debug(`Inbound forwarding closed: ${err.message}`);
      });
      pipeline(localSocket, remoteStream, (err) => {
        if (err) logger.debug(`Outbound forwarding closed: ${err.message}`);
      });
    });

    localSocket.on("error", (err) => {
      logger.error(`TCP connection error: ${err.message}`);
      if (!accepted) rejectConnection();
    });
  });

  return promise;
}

export const sshClientFactory: SshClientFactory = function ({
  username,
  host,
  port = 22,
  privateKey,
  localPort,
}: SshClientProps): Promise<SshClient> {
  const { promise, reject, resolve } = Promise.withResolvers<SshClient>();
  const client = new Client();
  const connection = new SshClientImpl(client);

  client.on("ready", async () => {
    this.logger.log("SSH connection ready");
    try {
      if (localPort !== undefined) {
        await setupPortForward(client, connection, localPort, this.logger);
      }
      resolve(connection);
    } catch (err) {
      reject(err);
    }
  });

  client.on("error", (err) => reject(new SshConnectionError(err.message)));
  client.on("close", () => reject(new SshConnectionError("SSH connection closed")));

  client.connect({
    host,
    username,
    port: Number(port),
    privateKey,
  });

  return promise;
};
