import { NodeCrypto } from "@effect/platform-node"
import { PgClient } from "@effect/sql-pg"
import { assert, describe, expect, it } from "@effect/vitest"
import { Cause, Effect, Exit, Layer, Redacted } from "effect"
import {
  MessageStorage,
  Runner,
  RunnerAddress,
  RunnerStorage,
  ShardingConfig,
  Snowflake,
  SqlMessageStorage,
  SqlRunnerStorage
} from "effect/cluster"
import { SqlClient, SqlError } from "effect/sql"
import { PgContainer } from "../fixtures/pg-utils.ts"
import { makeRequest } from "./MessageStorageTest.ts"

// A role that may read and write the cluster tables but create nothing.
const runtimeRole = "cluster_runtime"

const clientAs = (user?: string) =>
  Layer.unwrap(Effect.gen(function*() {
    const url = new URL((yield* PgContainer).getConnectionUri())
    if (user !== undefined) {
      url.username = user
      url.password = user
    }
    return PgClient.layer({ url: Redacted.make(url.toString()) })
  }))

const storagesFor = (prefix: string, migrate?: boolean) =>
  Layer.mergeAll(
    SqlMessageStorage.layerWith({ prefix, migrate }),
    SqlRunnerStorage.layerWith({ prefix, migrate })
  ).pipe(
    Layer.provide([Snowflake.layerGenerator, ShardingConfig.layerDefaults, NodeCrypto.layer]),
    Layer.provide(clientAs(runtimeRole))
  )

/** Applies both migrations as the owner and grants the runtime role DML only. */
const migrateAsOwner = (prefix: string) =>
  Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient
    yield* SqlMessageStorage.migrate({ prefix })
    yield* SqlRunnerStorage.migrate({ prefix })
    yield* sql.unsafe(`GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO ${runtimeRole}`)
    yield* sql.unsafe(`GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO ${runtimeRole}`)
  })

const RuntimeRole = Layer.effectDiscard(Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient
  yield* sql.unsafe(`CREATE ROLE ${runtimeRole} LOGIN PASSWORD '${runtimeRole}'`)
  yield* sql.unsafe(`REVOKE CREATE ON SCHEMA public FROM PUBLIC`)
}))

describe("SqlMessageStorage.migrate / SqlRunnerStorage.migrate", () => {
  it.layer(RuntimeRole.pipe(Layer.provideMerge(clientAs()), Layer.provideMerge(PgContainer.layer)), {
    concurrent: false,
    timeout: 120_000
  })("pg", (it) => {
    it.effect("a role without DDL privileges runs on tables applied by migrate", () =>
      Effect.gen(function*() {
        yield* migrateAsOwner("owned")

        const unmanaged = yield* Layer.build(storagesFor("owned")).pipe(Effect.scoped, Effect.exit)
        assert(Exit.isFailure(unmanaged))
        const denied = Cause.squash(unmanaged.cause)
        assert(SqlError.isSqlError(denied))
        expect(denied.reason._tag).toEqual("AuthorizationError")

        yield* Effect.gen(function*() {
          const messages = yield* MessageStorage.MessageStorage
          const runners = yield* RunnerStorage.RunnerStorage
          const runner = Runner.make({
            address: RunnerAddress.make("localhost", 1234),
            groups: ["default"],
            weight: 1
          })
          yield* runners.register(runner, true)
          expect(yield* runners.getRunners).toEqual([[runner, true]])
          const request = yield* makeRequest()
          expect((yield* messages.saveRequest(request))._tag).toEqual("Success")
        }).pipe(Effect.provide(Layer.merge(storagesFor("owned", false), Snowflake.layerGenerator)))
      }))

    it.effect("migrate: false refuses to start when a migration has not been applied", () =>
      Effect.gen(function*() {
        const sql = yield* SqlClient.SqlClient
        yield* migrateAsOwner("behind")
        yield* sql`DELETE FROM behind_migrations WHERE migration_id = 3`

        const exit = yield* Layer.build(storagesFor("behind", false)).pipe(Effect.scoped, Effect.exit)
        assert(Exit.isFailure(exit))
        expect(String(Cause.squash(exit.cause))).toMatch(/not applied to behind_migrations: 3_pg_messages_rowid_index/)
      }))
  })
})
