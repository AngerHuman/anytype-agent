import { Module } from "@nestjs/common";
import { ClsModule } from "nestjs-cls";
import { ClientModule } from "./client/client.module";
import { ConfigModule } from "./config/config.module";
import { LlmModule } from "./llm/llm.module";
import { ObserverModule } from "./observer/observer.module";

@Module({
  imports: [
    ConfigModule,
    ClsModule.forRoot({
      global: true,
    }),
    ClientModule,
    ObserverModule,
    LlmModule,
  ],
})
export class AppModule {}
