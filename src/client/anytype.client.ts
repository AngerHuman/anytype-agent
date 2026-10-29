import { Logger } from "@nestjs/common";
import { EventSourceParserStream } from "eventsource-parser/stream";
import { defer, firstValueFrom, from, type Observable, timer } from "rxjs";
import { finalize, map, retry, switchMap, tap } from "rxjs/operators";
import type { Static, TSchema } from "typebox";
import Value from "typebox/value";
import type { AppConfig } from "../config/config.schema";

/**
 * Transport layer for Anytype daemon: HTTP, headers, authentication, SSE streaming.
 * Has no knowledge of domain entities (spaces, chats, types) — boundary invariant.
 */
export class AnytypeClient {
  constructor(
    private readonly logger: Logger,
    readonly baseUrl: string,
    private readonly apiKey: string,
    private readonly apiVersion: string,
  ) {}

  async request<T extends TSchema>(
    schema: T,
    path: string,
    init: RequestInit = {},
  ): Promise<Static<T>> {
    const res = await this.requestRaw(path, init);

    if (!res.ok) {
      const errorBody = await res.text().catch(() => "");
      throw new Error(
        `[AnytypeClient ${res.status}] ${res.statusText}: ${errorBody || "Request failed"}`,
      );
    }

    let raw: unknown;
    try {
      // 200 responses for mutations may arrive with an empty body — spec does not define a schema
      const text = await res.text();
      raw = text.length > 0 ? JSON.parse(text) : {};
    } catch (jsonErr: unknown) {
      const message = jsonErr instanceof Error ? jsonErr.message : String(jsonErr);
      throw new Error(`[AnytypeClient] Failed to parse JSON response from ${path}: ${message}`);
    }

    try {
      return Value.Parse(schema, raw);
    } catch (_err: unknown) {
      const errors = [...Value.Errors(schema, raw)]
        .map((e) => `  - ${e.instancePath || "/"}: ${e.message} (schema: ${e.schemaPath})`)
        .join("\n");
      throw new Error(
        `[AnytypeClient] Schema validation failed for response from ${path}:\n${errors}`,
      );
    }
  }

  async requestRaw(path: string, init: RequestInit = {}): Promise<Response> {
    const url = `${this.baseUrl}${path.startsWith("/") ? path : `/${path}`}`;
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      "Anytype-Version": this.apiVersion,
      Authorization: `Bearer ${this.apiKey}`,
      ...(init.headers as Record<string, string>),
    };

    let res: Response;
    try {
      res = await fetch(url, {
        ...init,
        headers,
      });
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      throw new Error(`Connection failed to ${url}: ${message}`);
    }

    return res;
  }

  async get<T extends TSchema>(schema: T, path: string, init?: RequestInit): Promise<Static<T>> {
    return this.request(schema, path, { ...init, method: "GET" });
  }

  async post<T extends TSchema>(
    schema: T,
    path: string,
    body?: unknown,
    init?: RequestInit,
  ): Promise<Static<T>> {
    return this.request(schema, path, {
      ...init,
      method: "POST",
      body: body !== undefined ? JSON.stringify(body) : undefined,
    });
  }

  async patch<T extends TSchema>(
    schema: T,
    path: string,
    body?: unknown,
    init?: RequestInit,
  ): Promise<Static<T>> {
    return this.request(schema, path, {
      ...init,
      method: "PATCH",
      body: body !== undefined ? JSON.stringify(body) : undefined,
    });
  }

  async delete<T extends TSchema>(schema: T, path: string, init?: RequestInit): Promise<Static<T>> {
    return this.request(schema, path, { ...init, method: "DELETE" });
  }

  stream(path: string): Observable<{ event?: string; data: unknown }> {
    return defer(() => {
      const abortController = new AbortController();
      const url = `${this.baseUrl}${path.startsWith("/") ? path : `/${path}`}`;
      const headers = {
        Accept: "text/event-stream",
        "Anytype-Version": this.apiVersion,
        Authorization: `Bearer ${this.apiKey}`,
      };

      this.logger.log(`🔌 [AnytypeClient SSE] Subscribing to: ${url}`);

      return from(this.getRawStream(url, headers, abortController.signal)).pipe(
        switchMap((eventStream) => from(eventStream)),
        map((event) => {
          let parsedData: unknown;
          try {
            parsedData = JSON.parse(event.data);
          } catch {
            parsedData = event.data;
          }
          return { event: event.event, data: parsedData };
        }),
        finalize(() => {
          this.logger.log(`🛑 [AnytypeClient SSE] Aborting connection for ${path}`);
          abortController.abort();
        }),
      );
    });
  }

  private async getRawStream(
    url: string,
    headers: Record<string, string>,
    signal: AbortSignal,
  ): Promise<ReadableStream> {
    return fetch(url, { headers, signal }).then(async (res) => {
      if (!res.ok || !res.body) {
        const errorBody = await res.text().catch(() => "");
        throw new Error(
          `[AnytypeClient SSE ${res.status}] Failed to connect: ${errorBody || res.statusText}`,
        );
      }

      this.logger.log(`✅ [AnytypeClient SSE] Stream established with status ${res.status}`);

      return res.body
        .pipeThrough(new TextDecoderStream())
        .pipeThrough(new EventSourceParserStream());
    });
  }

  /**
   * Healthcheck: fails immediately on 401/403 without retries.
   */
  async checkHealth(
    healthcheckPath = "/v1/spaces",
    maxAttempts = 10,
    intervalMs = 2000,
  ): Promise<void> {
    const ready$ = defer(() =>
      fetch(`${this.baseUrl}${healthcheckPath}`, {
        headers: {
          "Anytype-Version": this.apiVersion,
          Authorization: `Bearer ${this.apiKey}`,
        },
      }),
    ).pipe(
      tap((res) => {
        if (res.status === 401 || res.status === 403) {
          throw new Error(`[AnytypeClient] Authentication failed with status ${res.status}`);
        }
        if (!res.ok) {
          throw new Error(`[AnytypeClient] Healthcheck returned status ${res.status}`);
        }
      }),
      retry({
        count: maxAttempts - 1,
        delay: (error: unknown, retryCount: number) => {
          const msg = error instanceof Error ? error.message : String(error);
          if (msg.includes("Authentication failed")) {
            throw error;
          }
          this.logger.log(
            `⏳ [AnytypeClient] Waiting for Anytype API (${this.baseUrl})... (attempt ${retryCount}/${maxAttempts})`,
          );
          return timer(intervalMs);
        },
      }),
      tap(() => {
        this.logger.log(
          `✅ [AnytypeClient] Successfully connected to Anytype API at ${this.baseUrl}`,
        );
      }),
    );

    try {
      await firstValueFrom(ready$);
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      throw new Error(
        `[AnytypeClient] API at ${this.baseUrl} is still unreachable after ${maxAttempts} attempts: ${message}`,
      );
    }
  }

  static async factory(config: AppConfig) {
    const log = new Logger("AnytypeClient");
    const apiUrl = config.ANYTYPE.API_URL;

    log.log(`Initializing Anytype client for ${apiUrl}...`);
    const client = new AnytypeClient(
      log,
      apiUrl.replace(/\/$/, ""),
      config.ANYTYPE.API_KEY,
      config.ANYTYPE.API_VERSION,
    );

    // Block bootstrap until API is reachable and token is valid
    await client.checkHealth();
    log.log(`Anytype client verified and ready at ${apiUrl}`);
    return client;
  }
}
