import { createMCPClient, type MCPClient } from "@ai-sdk/mcp";
import { Experimental_StdioMCPTransport } from "@ai-sdk/mcp/mcp-stdio";
import { Inject, Injectable, Logger, type OnModuleDestroy } from "@nestjs/common";
import { APP_CONFIG } from "../config/config.module";
import type { AppConfig } from "../config/config.schema";
import type { AnytypeClient } from "./anytype.client";
import { ANYTYPE_CLIENT } from "./client.constants";

@Injectable()
export class AnytypeMcp implements OnModuleDestroy {
  private clientPromise?: Promise<MCPClient>;
  private _client?: MCPClient;
  private readonly logger = new Logger(this.constructor.name);

  public get client(): MCPClient {
    if (!this._client) throw new Error("AnytypeMcp is not started. Call start() first.");

    return this._client;
  }

  constructor(
    @Inject(APP_CONFIG) private readonly config: AppConfig,
    @Inject(ANYTYPE_CLIENT) _clientService: AnytypeClient,
  ) {}

  async start(): Promise<void> {
    this.clientPromise ??= this.initClient().catch((err) => {
      this.clientPromise = undefined;
      throw err;
    });

    this._client = await this.clientPromise;
  }

  private async initClient(): Promise<MCPClient> {
    this.logger.log("Starting Anytype MCP client...");
    return createMCPClient({
      transport: new Experimental_StdioMCPTransport({
        command: "bun",
        args: ["run", "anytype-mcp"],
        env: {
          ANYTYPE_API_BASE_URL: this.config.ANYTYPE.API_URL,
          OPENAPI_MCP_HEADERS: JSON.stringify({
            Authorization: `Bearer ${this.config.ANYTYPE.API_KEY}`,
            "Anytype-Version": this.config.ANYTYPE.API_VERSION,
          }),
        },
      }),
    });
  }

  async stop(): Promise<void> {
    if (!this._client) return;

    this.logger.log("Stopping Anytype MCP client...");
    await this._client.close();
    this._client = undefined;
    this.clientPromise = undefined;
  }

  async onModuleDestroy(): Promise<void> {
    await this.stop();
  }
}
