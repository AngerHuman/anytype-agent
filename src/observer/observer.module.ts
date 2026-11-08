import { Module } from "@nestjs/common";
import { ClientModule } from "../client/client.module";
import { LlmModule } from "../llm/llm.module";
import { ChatsObserverFactory } from "./chats.observer";
import { OBSERVERS } from "./constants";
import { ObserverService } from "./observer.service";

@Module({
  imports: [ClientModule, LlmModule],
  providers: [
    ChatsObserverFactory,
    {
      provide: OBSERVERS,
      inject: [ChatsObserverFactory],
      useFactory: (...args) => args,
    },
    ObserverService,
  ],
  exports: [ObserverService],
})
export class ObserverModule {}
