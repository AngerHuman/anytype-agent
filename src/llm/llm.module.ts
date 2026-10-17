import { parse } from "node:path";
import { Module } from "@nestjs/common";
import { ClientModule } from "../client";
import { AnytypeProxy } from "../client/anytype.proxy";
import { APP_CONFIG } from "../config/config.module";
import type { AppConfig } from "../config/config.schema";
import type { SshClient } from "../ssh/ssh.client";
import { SshModule } from "../ssh/ssh.module";
import { SshService } from "../ssh/ssh.service";
import { AgyCliProvider } from "./cli/agy";
import { OpenCodeCliProvider } from "./cli/opencode";
import { LLM_PROVIDERS } from "./llm.constants";
import { LlmService } from "./llm.service";

@Module({
  imports: [ClientModule, SshModule],
  providers: [
    {
      provide: LLM_PROVIDERS,
      inject: [APP_CONFIG, SshService, AnytypeProxy],
      useFactory: (config: AppConfig, ssh: SshService, proxy: AnytypeProxy) => {
        return Promise.allSettled(
          config.LLM.map(async (provider) => {
            if (provider.MODE === "cli") {
              await proxy.start();
              let port = proxy.port;
              if (!port) throw new Error("proxy port not available");

              // TODO: add a local process spawner here later
              let spawner: SshClient | undefined;

              if (provider.SSH) {
                spawner = await ssh.connect(provider.SSH, provider.SSH_KEY, {
                  localPort: port,
                });

                const remotePort = spawner.forwardPort?.remotePort;
                if (remotePort) port = remotePort;
              }

              if (!spawner) throw new Error("Local spawner not implemented yet");

              const binaryName = parse(provider.CLI).name.toLowerCase();
              const apiParams = { port, docs: proxy.routesDoc };
              const cliParams = { binaryPath: provider.CLI, model: provider.MODEL };

              switch (binaryName) {
                case "agy":
                  return new AgyCliProvider(spawner, config, proxy, apiParams, provider.CLI);
                case "opencode":
                  return new OpenCodeCliProvider(spawner, config, proxy, apiParams, cliParams);
                default:
                  throw new Error(`Unsupported CLI provider binary: ${provider.CLI}`);
              }
            }
          }),
          // TODO: add a logger here to track the results
        ).then((results) => results.filter((r) => r.status === "fulfilled").map((r) => r.value));
      },
    },
    LlmService,
  ],
  exports: [LlmService],
})
export class LlmModule {}
