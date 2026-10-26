import {
  Inject,
  Injectable,
  Logger,
  type OnApplicationBootstrap,
  type OnModuleDestroy,
} from "@nestjs/common";
import { type Subscription, switchMap, timer } from "rxjs";
import { AnytypeService } from "../client/anytype.service";
import type { Member } from "../client/schema";
import { APP_CONFIG } from "../config/config.module";
import type { AppConfig } from "../config/config.schema";
import { OBSERVERS } from "./constants";
import type { AbstractObserver, ObserverFactory } from "./types";

const WRITE_ROLES = new Set(["editor", "admin", "owner"]);

@Injectable()
export class ObserverService implements OnApplicationBootstrap, OnModuleDestroy {
  private readonly log = new Logger(this.constructor.name);
  private readonly botName: string;
  private sub: Subscription | undefined;
  private readonly registry = new Map<string, AbstractObserver[]>();
  private readonly lastActivity = new Map<string, number>();

  constructor(
    private readonly anytype: AnytypeService,
    @Inject(OBSERVERS) private readonly observers: ObserverFactory[],
    @Inject(APP_CONFIG) private readonly config: AppConfig,
  ) {
    this.botName = config.ANYTYPE.BOT_NAME;
  }

  private findActiveBot(spaceName: string, members: Member[]): Member | null {
    try {
      // Nameless spaces are technical spaces, skip them.
      if (!spaceName.length) return null;

      const bot = members.find(
        (m) => m.name.toLowerCase() === this.botName.toLowerCase() || m.identity === this.botName,
      );

      if (bot?.status !== "active") return null;
      if (!WRITE_ROLES.has(bot.role)) {
        this.log.warn(`⛔ [Space:${spaceName}] Bot role "${bot.role}" is read-only. Skipping.`);
        return null;
      }
      return bot;
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      this.log.error(`Failed to check members in space "${spaceName}": ${msg}`);
      return null;
    }
  }

  private handleObserverDeath(spaceId: string, err: unknown): void {
    const observers = this.registry.get(spaceId);
    if (!observers) return;

    const msg = err instanceof Error ? err.message : String(err);
    this.log.error(`❌ Fatal observer failure in space "${spaceId}": ${msg}`);

    for (const obs of observers) {
      obs.destroy();
    }
    this.registry.delete(spaceId);
  }

  private async checkSpaces() {
    const spaces = await this.anytype.getSpacesWithMembers();
    const activeSpaceIds = new Set<string>();

    for (const { id, name, members } of spaces) {
      activeSpaceIds.add(id);

      const isConnected = this.registry.has(id);
      if (isConnected) continue;

      const bot = this.findActiveBot(name, members);
      if (!bot) continue;

      const observers = this.observers.map((factory) => {
        return factory.create(id, this.botName, bot.identity);
      });

      this.registry.set(id, observers);

      for (const observer of observers) {
        observer.run().subscribe({
          next: () => this.lastActivity.set(id, Date.now()),
          error: (err) => this.handleObserverDeath(id, err),
        });
      }
    }

    for (const spaceId of this.registry.keys()) {
      const observers = this.registry.get(spaceId) ?? [];

      if (!activeSpaceIds.has(spaceId)) {
        this.log.debug(`Space ${spaceId} is not active, removing from registry`);

        observers.forEach((obs) => {
          obs.destroy();
        });
        this.registry.delete(spaceId);
      }
    }
  }

  onApplicationBootstrap(): void {
    this.sub = timer(0, this.config.SETTINGS.SCAN_INTERVAL_MS)
      .pipe(switchMap(() => this.checkSpaces()))
      .subscribe();
  }

  onModuleDestroy(): void {
    this.registry.forEach((observers) => {
      observers.forEach((observer) => {
        observer.destroy();
      });
    });

    if (this.sub) this.sub.unsubscribe();
  }
}
