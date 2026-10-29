import { Global, Module } from "@nestjs/common";
import { loadFromEnv } from "./config.load";

export const APP_CONFIG = Symbol("AppConfig");

@Global()
@Module({
  providers: [
    {
      provide: APP_CONFIG,
      useFactory: () => {
        // Extend here when YAML config support is added
        const env = loadFromEnv();
        return Object.freeze(env);
      },
    },
  ],
  exports: [APP_CONFIG],
})
export class ConfigModule {}
