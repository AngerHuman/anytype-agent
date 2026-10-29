import "reflect-metadata";
import { afterEach, beforeEach, describe, expect, it, mock } from "bun:test";
import { Test, type TestingModule } from "@nestjs/testing";
import { type ExecOptions, SshClient, type SshProcess } from "../ssh.client";
import { SSH_CLIENT_FACTORY, SSH_CLIENTS_MAP } from "../ssh.constants";
import { SshModule } from "../ssh.module";
import { SshService } from "../ssh.service";

class FakeSshClient extends SshClient {
  spawn = mock(
    async (_cmd: string, _args: string[] = [], _opts: ExecOptions = {}): Promise<SshProcess> => {
      return {} as SshProcess;
    },
  );

  override onClose = mock(() => {});
}

describe("SshModule & SshService (NestJS Integration Tests)", () => {
  let testingModule: TestingModule;
  let service: SshService;
  let fakeClient: FakeSshClient;
  let mockFactory: ReturnType<typeof mock>;

  beforeEach(async () => {
    fakeClient = new FakeSshClient();
    mockFactory = mock(async (_props) => fakeClient);

    testingModule = await Test.createTestingModule({
      imports: [SshModule],
    })
      .overrideProvider(SSH_CLIENT_FACTORY)
      .useValue(mockFactory)
      .compile();

    service = testingModule.get(SshService);

    // Mock loadPrivateKey to avoid accessing real filesystem in unit tests
    (service as unknown as { loadPrivateKey: (path: string) => Promise<string> }).loadPrivateKey =
      mock(async () => "MOCK_PRIVATE_KEY");
  });

  afterEach(async () => {
    await testingModule?.close();
  });

  it("1. URI validation: invalid SSH URIs throw 'Invalid SSH URI' error", async () => {
    const invalidUris = [
      "http://not-ssh:22",
      "invalid-uri",
      "ssh://bot@:22", // empty hostname
      "ssh://@host:22", // empty username
      "ssh://bot@host:99999", // port out of range (> 65535)
      "ssh://bot@host:0", // port 0 is invalid for remote host
      "ssh://bot@host:-1", // negative port
      "ssh://bot@host:abc", // non-numeric port
    ];

    for (const uri of invalidUris) {
      expect(service.connect(uri, "/key/path")).rejects.toThrow("Invalid SSH URI");
    }

    expect(mockFactory).not.toHaveBeenCalled();
  });

  it("2. Parsing and factory invocation: passes correct props to factory", async () => {
    const client = await service.connect("ssh://bot@my-vps.internal:2222", "/id_ed25519", {
      localPort: 8080,
    });

    expect(client).toBe(fakeClient);
    expect(mockFactory).toHaveBeenCalledTimes(1);
    expect(mockFactory).toHaveBeenCalledWith({
      username: "bot",
      host: "my-vps.internal",
      port: 2222,
      privateKey: "MOCK_PRIVATE_KEY",
      localPort: 8080,
    });
  });

  it("3. Single-flight / deduplication: concurrent connect() calls invoke factory exactly once", async () => {
    const [c1, c2, c3] = await Promise.all([
      service.connect("ssh://admin@cluster:22", "/key"),
      service.connect("ssh://admin@cluster:22", "/key"),
      service.connect("ssh://admin@cluster:22", "/key"),
    ]);

    expect(c1).toBe(c2);
    expect(c2).toBe(c3);
    expect(mockFactory).toHaveBeenCalledTimes(1);
  });

  it("4. Cache invalidation: closing client removes session from cache", async () => {
    const c1 = await service.connect("ssh://user@server:22", "/key");
    expect(mockFactory).toHaveBeenCalledTimes(1);

    // Close SSH client (emits closed$)
    c1.close();

    // Next connect call should trigger factory again
    const c2 = await service.connect("ssh://user@server:22", "/key");
    expect(c2).toBe(fakeClient);
    expect(mockFactory).toHaveBeenCalledTimes(2);
  });

  it("5. Cache cleanup on failure: does not retain rejected connection promise", async () => {
    mockFactory.mockImplementationOnce(async () => {
      throw new Error("SSH Connection Refused");
    });

    // First attempt fails
    expect(service.connect("ssh://fail@host:22", "/key")).rejects.toThrow("SSH Connection Refused");

    // Retry should not return rejected promise but attempt a new connection
    mockFactory.mockImplementationOnce(async () => fakeClient);
    const retryClient = await service.connect("ssh://fail@host:22", "/key");
    expect(retryClient).toBe(fakeClient);
    expect(mockFactory).toHaveBeenCalledTimes(2);
  });

  it("6. Cache keys differentiate by localPort", async () => {
    await service.connect("ssh://user@server:22", "/key");
    await service.connect("ssh://user@server:22", "/key", { localPort: 8080 });
    await service.connect("ssh://user@server:22", "/key", { localPort: 9090 });

    expect(mockFactory).toHaveBeenCalledTimes(3);
    expect(mockFactory).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({ localPort: undefined }),
    );
    expect(mockFactory).toHaveBeenNthCalledWith(2, expect.objectContaining({ localPort: 8080 }));
    expect(mockFactory).toHaveBeenNthCalledWith(3, expect.objectContaining({ localPort: 9090 }));
  });

  it("7. Cache deletion checks connection identity before removing", async () => {
    const clientsMap = testingModule.get<Map<string, Promise<SshClient>>>(SSH_CLIENTS_MAP);

    const oldClient = new FakeSshClient();
    mockFactory.mockImplementationOnce(async () => oldClient);
    const c1 = await service.connect("ssh://user@server:22", "/key");

    // Manually simulate a newer connection replacing the map key
    const newClient = new FakeSshClient();
    const newConnectionPromise = Promise.resolve(newClient);
    const mapKey = "server:22:user:/key:none";
    clientsMap.set(mapKey, newConnectionPromise);

    // Old client closes; should NOT delete the newer connection from map
    c1.close();

    expect(clientsMap.get(mapKey)).toBe(newConnectionPromise);
  });

  it("8. onModuleDestroy closes all active connections and clears the map", async () => {
    const client1 = new FakeSshClient();
    const client2 = new FakeSshClient();

    mockFactory.mockImplementationOnce(async () => client1);
    await service.connect("ssh://user1@server:22", "/key1");

    mockFactory.mockImplementationOnce(async () => client2);
    await service.connect("ssh://user2@server:22", "/key2");

    const clientsMap = testingModule.get<Map<string, Promise<SshClient>>>(SSH_CLIENTS_MAP);
    expect(clientsMap.size).toBe(2);

    await service.onModuleDestroy();

    expect(client1.onClose).toHaveBeenCalledTimes(1);
    expect(client2.onClose).toHaveBeenCalledTimes(1);
    expect(clientsMap.size).toBe(0);

    // Calling close() again must be a no-op (idempotent)
    client1.close();
    expect(client1.onClose).toHaveBeenCalledTimes(1);
  });
});
