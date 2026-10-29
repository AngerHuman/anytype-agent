import { randomInt } from "node:crypto";
import { Inject, Injectable, Logger, type OnModuleDestroy } from "@nestjs/common";
import {
  catchError,
  firstValueFrom,
  from,
  map,
  Observable,
  of,
  Subject,
  switchMap,
  tap,
} from "rxjs";
import { APP_CONFIG } from "../config/config.module";
import type { AppConfig } from "../config/config.schema";
import type { AnytypeClient } from "./anytype.client";
import { ANYTYPE_CLIENT } from "./client.constants";
import { SPACE_ROUTES_DOC } from "./routes";

export type ProxyTrace = {
  alias: string;
  method: string;
  path: string;
  status: number;
  durationMs: number;
  error?: string;
};

@Injectable()
export class AnytypeProxy implements OnModuleDestroy {
  private readonly logger = new Logger(this.constructor.name);
  private readonly spaceAliases = new Map<string, string>();
  private readonly ALIAS_REGEX = /^\/v1\/spaces\/(\d{6})(\/.*)?$/;

  private server?: ReturnType<typeof Bun.serve>;

  readonly routesDoc = SPACE_ROUTES_DOC;
  readonly traces$ = new Subject<ProxyTrace>();

  public get port(): number | undefined {
    return this.server?.port;
  }

  constructor(
    @Inject(APP_CONFIG) private readonly config: AppConfig,
    @Inject(ANYTYPE_CLIENT) private readonly client: AnytypeClient,
  ) {}

  async start(): Promise<void> {
    if (this.server) return;

    const targetUrl = this.config.ANYTYPE.API_URL;

    try {
      this.server = Bun.serve({
        port: 0,
        fetch: (req) => firstValueFrom(this.proxyRequest$(req)),
      });

      this.logger.log(
        `🚀 Started reverse proxy on http://127.0.0.1:${this.server.port} -> ${targetUrl}`,
      );
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      this.logger.error(`❌ Failed to start proxy on port ${this.server?.port}: ${msg}`);
      throw err;
    }
  }

  stop(): void {
    if (this.server) {
      this.server.stop(true);
      this.server = undefined;
      this.logger.log(`🛑 Reverse proxy stopped`);
    }
  }

  onModuleDestroy(): void {
    this.stop();
  }

  private parseAlias(pathname: string): string | null {
    const match = this.ALIAS_REGEX.exec(pathname);
    return match?.[1] ?? null;
  }

  private proxyRequest$(req: Request): Observable<Response> {
    return of({ req, startTime: performance.now() }).pipe(
      map(({ req, startTime }) => {
        const url = new URL(req.url);
        const alias = this.parseAlias(url.pathname);
        const spaceId = alias ? this.spaceAliases.get(alias) : null;

        if (!alias || !spaceId) {
          throw this.NOT_FOUND;
        }

        const targetUrl = `${url.pathname.replace(alias, spaceId)}${url.search}`;

        return { req, alias, targetUrl, startTime };
      }),
      switchMap((ctx) => {
        const {
          req: { method, body },
          targetUrl,
        } = ctx;

        return from(this.client.requestRaw(targetUrl, { method, body })).pipe(
          map((res) => ({ ...ctx, res })),
        );
      }),
      tap(({ alias, req, res, startTime }) => {
        this.traces$.next({
          alias,
          method: req.method,
          path: new URL(req.url).pathname,
          status: res.status,
          durationMs: performance.now() - startTime,
        });
      }),
      map(({ res }) => res),
      catchError((err) => {
        if (err instanceof Response) {
          const url = new URL(req.url);
          const alias = this.parseAlias(url.pathname);
          const active = [...this.spaceAliases.entries()].map(([a, s]) => `${a}->${s}`).join(", ");
          this.logger.warn(
            `⚠️ Proxy rejected [${err.status}]: ${req.method} ${url.pathname} (parsed alias: ${alias || "none"}, active: [${active}])`,
          );
          return of(err);
        }

        this.logger.error(`❌ Proxy error: ${err instanceof Error ? err.message : String(err)}`);
        return of(new Response(null, { status: 500 }));
      }),
    );
  }

  /**
   * Issues a 6-digit alias instead of the real spaceId for a single LLM job.
   *
   * Lifecycle SSOT: the alias lives strictly for the duration of the run — issued at the start of
   * run() and revoked via finalize on all exit paths (complete/error/unsubscribe).
   * TTL is unnecessary and harmful here: a second source of truth for alias expiration
   * drops long runs with 404s mid-work. SpaceId (aliased and real) is not a secret:
   * useless without an API key, and the agent never bypasses the proxy.
   */
  issueSpaceAlias(spaceId: string): string {
    let alias: string;
    do {
      alias = randomInt(100_000, 1_000_000).toString();
    } while (this.spaceAliases.has(alias));

    this.spaceAliases.set(alias, spaceId);
    this.logger.log(`🔑 Issued space alias: ${alias} -> ${spaceId}`);
    return alias;
  }

  revokeSpaceAlias(alias: string): void {
    this.logger.log(`🔒 Revoking space alias: ${alias}`);
    this.spaceAliases.delete(alias);
  }

  private get NOT_FOUND(): Response {
    return new Response(JSON.stringify({ error: "not found" }), {
      status: 404,
      headers: { "Content-Type": "application/json" },
    });
  }
}
