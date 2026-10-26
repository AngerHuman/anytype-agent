import { Module } from "@nestjs/common";
import { APP_CONFIG } from "../config/config.module";
import { AnytypeClient } from "./anytype.client";
import { AnytypeMcp } from "./anytype.mcp";
import { AnytypeProxy } from "./anytype.proxy";
import { AnytypeService } from "./anytype.service";
import { ANYTYPE_CLIENT } from "./client.constants";

@Module({
  providers: [
    {
      provide: ANYTYPE_CLIENT,
      inject: [APP_CONFIG],
      useFactory: AnytypeClient.factory,
    },
    AnytypeService,
    AnytypeProxy,
    AnytypeMcp,
  ],
  exports: [AnytypeService, AnytypeProxy, AnytypeMcp],
})
export class ClientModule {}
