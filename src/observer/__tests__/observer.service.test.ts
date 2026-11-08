import "reflect-metadata";
import { describe, expect, it, mock } from "bun:test";
import { throwError } from "rxjs";
import type { AnytypeService } from "../../client/anytype.service";
import type { Member } from "../../client/schema";
import { makeAppConfig } from "../../config/__tests__/fixtures";
import { ObserverService } from "../observer.service";
import type { AbstractObserver, ObserverFactory } from "../types";
import { callCount, makeFakeObserver, type ObserverServiceInternals } from "./helpers";

describe("ObserverService (Unit Tests)", () => {
  const createMockConfig = (botName = "TestBot", scanIntervalMs = 60000) =>
    makeAppConfig({
      ANYTYPE: { BOT_NAME: botName },
      SETTINGS: { SCAN_INTERVAL_MS: scanIntervalMs },
    });

  const makeSpaceWithMembers = (
    id: string,
    name: string,
    role = "editor",
    botName = "TestBot",
    status = "active",
  ) => ({
    id,
    name,
    members: [
      {
        identity: botName,
        name: botName,
        role,
        status,
      } as Member,
    ],
  });

  it("1. Order fix regression: synchronous error in run() leaves no zombies in registry", async () => {
    const destroySpy = mock(() => {});
    const fakeObserver = makeFakeObserver(
      throwError(() => new Error("sync boom")),
      destroySpy,
    );

    const fakeFactory: ObserverFactory = {
      create: mock(() => fakeObserver),
    };

    const anytypeFake = {
      getSpacesWithMembers: mock(async () => [
        makeSpaceWithMembers("space.boom", "Boom Space", "editor"),
      ]),
    } as unknown as AnytypeService;

    const svc = new ObserverService(anytypeFake, [fakeFactory], createMockConfig());
    const internals = svc as unknown as ObserverServiceInternals;

    await internals.checkSpaces();

    expect(internals.registry.has("space.boom")).toBe(false);
    expect(callCount(destroySpy)).toBe(1);
  });

  it("2. Roles and access: space with bot-editor -> observer created; viewer / no bot / empty name -> not created", async () => {
    const createdObservers: AbstractObserver[] = [];
    const fakeFactory: ObserverFactory = {
      create: mock((_id: string, _botName: string) => {
        const obs = makeFakeObserver();
        createdObservers.push(obs);
        return obs;
      }),
    };

    const spaces = [
      makeSpaceWithMembers("space.valid", "Valid Space", "editor"),
      makeSpaceWithMembers("space.admin", "Admin Space", "admin"),
      makeSpaceWithMembers("space.owner", "Owner Space", "owner"),
      makeSpaceWithMembers("space.viewer", "Viewer Space", "viewer"),
      makeSpaceWithMembers("space.emptyname", "", "editor"),
      {
        id: "space.nobot",
        name: "No Bot Space",
        members: [
          { identity: "other_user", name: "Alice", role: "editor", status: "active" } as Member,
        ],
      },
      makeSpaceWithMembers("space.inactive", "Inactive Bot Space", "editor", "TestBot", "invited"),
    ];

    const anytypeFake = {
      getSpacesWithMembers: mock(async () => spaces),
    } as unknown as AnytypeService;

    const svc = new ObserverService(anytypeFake, [fakeFactory], createMockConfig());
    const internals = svc as unknown as ObserverServiceInternals;

    await internals.checkSpaces();

    expect(internals.registry.has("space.valid")).toBe(true);
    expect(internals.registry.has("space.admin")).toBe(true);
    expect(internals.registry.has("space.owner")).toBe(true);
    expect(internals.registry.has("space.viewer")).toBe(false);
    expect(internals.registry.has("space.emptyname")).toBe(false);
    expect(internals.registry.has("space.nobot")).toBe(false);
    expect(internals.registry.has("space.inactive")).toBe(false);
    expect(callCount(fakeFactory.create)).toBe(3);
  });

  it("3. Missing space: second checkSpaces without space -> destroy called, registry cleaned up", async () => {
    const destroySpy = mock(() => {});
    const fakeFactory: ObserverFactory = {
      create: mock(() => makeFakeObserver(undefined, destroySpy)),
    };

    let spacesList = [makeSpaceWithMembers("space.1", "Space 1", "editor")];

    const anytypeFake = {
      getSpacesWithMembers: mock(async () => spacesList),
    } as unknown as AnytypeService;

    const svc = new ObserverService(anytypeFake, [fakeFactory], createMockConfig());
    const internals = svc as unknown as ObserverServiceInternals;

    // 1st scan: space is registered
    await internals.checkSpaces();
    expect(internals.registry.has("space.1")).toBe(true);
    expect(callCount(destroySpy)).toBe(0);

    // 2nd scan: space disappeared from Anytype
    spacesList = [];
    await internals.checkSpaces();
    expect(internals.registry.has("space.1")).toBe(false);
    expect(callCount(destroySpy)).toBe(1);
  });

  it("4. Duplicates: multiple checkSpaces with same space -> factory.create exactly 1 time", async () => {
    const fakeFactory: ObserverFactory = {
      create: mock(() => makeFakeObserver()),
    };

    const anytypeFake = {
      getSpacesWithMembers: mock(async () => [
        makeSpaceWithMembers("space.dup", "Duplicate Test Space", "editor"),
      ]),
    } as unknown as AnytypeService;

    const svc = new ObserverService(anytypeFake, [fakeFactory], createMockConfig());
    const internals = svc as unknown as ObserverServiceInternals;

    await internals.checkSpaces();
    await internals.checkSpaces();
    await internals.checkSpaces();

    expect(callCount(fakeFactory.create)).toBe(1);
    expect(internals.registry.size).toBe(1);
  });
});
