import { Logger, Module } from "@nestjs/common";
import { type SshClient } from "./ssh.client";
import { sshClientFactory } from "./ssh.client/factory";
import { SSH_CLIENT_FACTORY, SSH_CLIENTS_MAP } from "./ssh.constants";
import { SshService } from "./ssh.service";

@Module({
  providers: [
    {
      provide: SSH_CLIENTS_MAP,
      useValue: new Map<string, Promise<SshClient>>(),
    },
    {
      provide: SSH_CLIENT_FACTORY,
      useFactory: () => {
        const logger = new Logger("SSH_CLIENT_FACTORY");
        return sshClientFactory.bind({ logger });
      },
    },
    SshService,
  ],
  exports: [SshService],
})
export class SshModule {}
