# Implementation Doc — Extract `NRTReplicaTranslogOps`

**Scope:** remove ~52 LOC of duplication between `NRTReplicationEngine` and `DataFormatAwareNRTReplicationEngine` by extracting the three translog-only write stubs (`index` / `delete` / `noOp`), the `TranslogEventListener` factory, and the `TranslogDeletionPolicy` factory into a new package-level utility class.

**Non-scope:** flush orchestration, close/lifecycle helpers, any shared base class. See §8.

---

## 1. Background

Both replica engines are write-disabled: operations arrive pre-assigned a seqno, go only to the translog, and are never indexed locally. The code to do this is duplicated byte-for-byte in both files except for an `Engine.` type prefix on the DFA side (because `DataFormatAwareNRTReplicationEngine` is not inside the `Engine` class so its inner-class references must be fully qualified).

The `TranslogEventListener` body in each constructor is identical (12 lines). The `TranslogDeletionPolicy` factory is identical (10 lines) — `Engine.getTranslogDeletionPolicy(EngineConfig)` and `DataFormatAwareNRTReplicationEngine.getTranslogDeletionPolicy()` have the same body.

None of this duplication is load-bearing in either direction, but each of these paths is in the hot write path and the three write stubs (`index`/`delete`/`noOp`) are exactly the kind of code that drifts silently between two files when one gets fixed and the other doesn't.

### Current duplication (verified against HEAD)

| Location | NRT | DFANRT | LOC |
|---|---|---|---|
| `index(Index)` body | `NRTReplicationEngine.java:244-253` | `DataFormatAwareNRTReplicationEngine.java:393-402` | 10 |
| `delete(Delete)` body | `NRTReplicationEngine.java:256-265` | `DataFormatAwareNRTReplicationEngine.java:405-414` | 10 |
| `noOp(NoOp)` body | `NRTReplicationEngine.java:268-277` | `DataFormatAwareNRTReplicationEngine.java:417-426` | 10 |
| `TranslogEventListener` anon inner class | `NRTReplicationEngine.java:119-133` | `DataFormatAwareNRTReplicationEngine.java:202-216` | 14 each (identical) |
| `getTranslogDeletionPolicy` | `Engine.java:968-981` (inherited by NRT) | `DataFormatAwareNRTReplicationEngine.java:899-912` | 12 |

**Total identical logic:** ~56 LOC present in two places (once in each engine, plus the `Engine` base for the deletion policy).

### Constraint that rules out inheritance

`NRTReplicationEngine extends Engine` (abstract class). `DataFormatAwareNRTReplicationEngine implements Indexer` (interface). They cannot share a common base without restructuring one of two major interface hierarchies (`Engine` subclasses and `Indexer` implementations). **Composition is the only viable path.**

---

## 2. Goals

1. **Single source of truth** for replica translog-only write semantics.
2. **Zero behavioral change.** The extraction is a mechanical refactor.
3. **Minimal new surface area.** One new package-private class, five static methods, no new interfaces other than a local `@FunctionalInterface` if needed.
4. **No test rewrites.** Existing `NRTReplicationEngineTests` and `DataFormatAwareEngineTests` / `DataFormatAware*IT` must pass unchanged.

---

## 3. Non-goals

Explicitly deferred to a separate RFC if ever needed (see §8 of `.sisyphus/handoff/nrt-extraction-plan.md` for full rationale):

- Extracting the `flush(boolean, boolean)` body — net +7 LOC for a 7-param helper; not worth it.
- Extracting `close` / `flushAndClose` / `awaitPendingClose` / `closeNoLock` — lifecycle-critical, marginal savings.
- Extracting `updateSegments` / `updateCatalogSnapshot` skeleton — the shared part is the flush-on-gen-change guard (~6 lines); the rest differs.
- A shared abstract base class — infeasible (see constraint above).
- The trivial no-op stubs (`activateThrottling`, `fillSeqNoGaps`, etc.) — 1 line each, not worth extracting.

---

## 4. New file

### 4.1 Path

`server/src/main/java/org/opensearch/index/engine/NRTReplicaTranslogOps.java`

Package-private class in `org.opensearch.index.engine`. This placement gives it direct access to `Engine.Index`/`Engine.Delete`/`Engine.NoOp`/`Engine.IndexResult`/`Engine.DeleteResult`/`Engine.NoOpResult` without qualified-reference clutter, and keeps it a true implementation detail of the replica-engine family.

### 4.2 API surface

```java
package org.opensearch.index.engine;

import java.io.IOException;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.seqno.LocalCheckpointTracker;
import org.opensearch.index.translog.DefaultTranslogDeletionPolicy;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogDeletionPolicy;
import org.opensearch.index.translog.TranslogException;
import org.opensearch.index.translog.WriteOnlyTranslogManager;
import org.opensearch.index.translog.listener.TranslogEventListener;

/**
 * Shared translog-only write + listener/factory helpers used by both
 * {@link NRTReplicationEngine} and
 * {@link DataFormatAwareNRTReplicationEngine}. Both engines receive
 * operations with pre-assigned seq-nos and only need to append them to
 * the translog; the three write methods are identical byte-for-byte
 * except for an {@code Engine.} type prefix on the DFA side.
 *
 * <p>All methods are stateless and take engine state via parameters.
 * Package-private: this is an implementation detail of the replica
 * engines, not a public API.
 */
final class NRTReplicaTranslogOps {

    private NRTReplicaTranslogOps() {}

    static Engine.IndexResult index(
        WriteOnlyTranslogManager translogManager,
        LocalCheckpointTracker localCheckpointTracker,
        Engine.Index index
    ) throws IOException {
        Engine.IndexResult r = new Engine.IndexResult(index.version(), index.primaryTerm(), index.seqNo(), false);
        Translog.Location location = translogManager.add(new Translog.Index(index, r));
        r.setTranslogLocation(location);
        r.setTook(System.nanoTime() - index.startTime());
        r.freeze();
        localCheckpointTracker.advanceMaxSeqNo(index.seqNo());
        return r;
    }

    static Engine.DeleteResult delete(
        WriteOnlyTranslogManager translogManager,
        LocalCheckpointTracker localCheckpointTracker,
        Engine.Delete delete
    ) throws IOException {
        Engine.DeleteResult r = new Engine.DeleteResult(delete.version(), delete.primaryTerm(), delete.seqNo(), true);
        Translog.Location location = translogManager.add(new Translog.Delete(delete, r));
        r.setTranslogLocation(location);
        r.setTook(System.nanoTime() - delete.startTime());
        r.freeze();
        localCheckpointTracker.advanceMaxSeqNo(delete.seqNo());
        return r;
    }

    static Engine.NoOpResult noOp(
        WriteOnlyTranslogManager translogManager,
        LocalCheckpointTracker localCheckpointTracker,
        Engine.NoOp noOp
    ) throws IOException {
        Engine.NoOpResult r = new Engine.NoOpResult(noOp.primaryTerm(), noOp.seqNo());
        Translog.Location location = translogManager.add(new Translog.NoOp(noOp.seqNo(), noOp.primaryTerm(), noOp.reason()));
        r.setTranslogLocation(location);
        r.setTook(System.nanoTime() - noOp.startTime());
        r.freeze();
        localCheckpointTracker.advanceMaxSeqNo(noOp.seqNo());
        return r;
    }

    /**
     * Creates the {@link TranslogEventListener} used by both replica engines. The listener forwards
     * translog failures to the engine's {@code failEngine} hook and trims unreferenced readers on
     * sync. The {@code translogManagerSupplier} lazily returns the manager reference so the
     * listener can be constructed before the manager field is assigned.
     */
    static TranslogEventListener createTranslogEventListener(
        BiConsumer<String, Exception> failEngine,
        Supplier<WriteOnlyTranslogManager> translogManagerSupplier,
        ShardId shardId
    ) {
        return new TranslogEventListener() {
            @Override
            public void onFailure(String reason, Exception ex) {
                failEngine.accept(reason, ex);
            }

            @Override
            public void onAfterTranslogSync() {
                try {
                    translogManagerSupplier.get().trimUnreferencedReaders();
                } catch (IOException ex) {
                    throw new TranslogException(shardId, "failed to trim unreferenced translog readers", ex);
                }
            }
        };
    }

    /**
     * Selects a translog deletion policy: the engine-config-provided custom factory if present,
     * otherwise {@link DefaultTranslogDeletionPolicy}. Centralised here so
     * {@link DataFormatAwareNRTReplicationEngine} and {@link Engine} share the same selection
     * logic.
     */
    static TranslogDeletionPolicy getTranslogDeletionPolicy(EngineConfig engineConfig) {
        TranslogDeletionPolicy custom = null;
        if (engineConfig.getCustomTranslogDeletionPolicyFactory() != null) {
            custom = engineConfig.getCustomTranslogDeletionPolicyFactory()
                .create(engineConfig.getIndexSettings(), engineConfig.retentionLeasesSupplier());
        }
        return Objects.requireNonNullElseGet(
            custom,
            () -> new DefaultTranslogDeletionPolicy(
                engineConfig.getIndexSettings().getTranslogRetentionSize().getBytes(),
                engineConfig.getIndexSettings().getTranslogRetentionAge().getMillis(),
                engineConfig.getIndexSettings().getTranslogRetentionTotalFiles()
            )
        );
    }
}
```

### 4.3 Design notes

- **`ensureOpen` is NOT passed in.** The call sites already guard with their own `ensureOpen()` as the first statement. Moving it into the helper would either shadow the real engine's open check or force the helper to know about `LifecycleAware`. Cleaner to let each caller keep its own guard and pass only what the helper actually needs.
- **`Supplier<WriteOnlyTranslogManager>` in the listener factory.** NRT uses `this::getLocalCheckpointTracker` in its current body; the translog manager self-reference is the same pattern. The supplier avoids a chicken-and-egg where the listener needs the manager before the manager is fully constructed.
- **`BiConsumer<String, Exception>` for failEngine.** Both engines have different method signatures/safety profiles for `failEngine` (see review doc); passing a method reference via `BiConsumer` lets each engine wire up its own version without coupling.
- **No `@FunctionalInterface`** — we're reusing the standard `BiConsumer` and `Supplier`. No custom SAM needed.
- **Package-private visibility.** Nothing outside `org.opensearch.index.engine` should call this. If a third replica engine ever appears, we can broaden then.

---

## 5. Call-site changes

### 5.1 `NRTReplicationEngine.java`

**Before (3 bodies, lines 244-278):**

```java
@Override
public IndexResult index(Index index) throws IOException {
    ensureOpen();
    IndexResult indexResult = new IndexResult(index.version(), index.primaryTerm(), index.seqNo(), false);
    final Translog.Location location = translogManager.add(new Translog.Index(index, indexResult));
    indexResult.setTranslogLocation(location);
    indexResult.setTook(System.nanoTime() - index.startTime());
    indexResult.freeze();
    localCheckpointTracker.advanceMaxSeqNo(index.seqNo());
    return indexResult;
}
// ...similar for delete and noOp
```

**After:**

```java
@Override
public IndexResult index(Index index) throws IOException {
    ensureOpen();
    return NRTReplicaTranslogOps.index(translogManager, localCheckpointTracker, index);
}

@Override
public DeleteResult delete(Delete delete) throws IOException {
    ensureOpen();
    return NRTReplicaTranslogOps.delete(translogManager, localCheckpointTracker, delete);
}

@Override
public NoOpResult noOp(NoOp noOp) throws IOException {
    ensureOpen();
    return NRTReplicaTranslogOps.noOp(translogManager, localCheckpointTracker, noOp);
}
```

**Before (ctor, lines 119-133):** inline anonymous `TranslogEventListener` block.

**After:**

```java
translogManagerRef = new WriteOnlyTranslogManager(
    engineConfig.getTranslogConfig(),
    engineConfig.getPrimaryTermSupplier(),
    engineConfig.getGlobalCheckpointSupplier(),
    getTranslogDeletionPolicy(engineConfig), // unchanged — Engine's inherited method
    shardId,
    readLock,
    this::getLocalCheckpointTracker,
    translogUUID,
    NRTReplicaTranslogOps.createTranslogEventListener(
        this::failEngine,
        () -> translogManager,
        shardId
    ),
    this,
    engineConfig.getTranslogFactory(),
    engineConfig.getStartedPrimarySupplier(),
    TranslogOperationHelper.create(engineConfig)
);
```

**Lines removed:** ~45 (three stubs ~30 + event-listener anon class ~15). **Lines added:** ~15 (three one-line delegations + 5-line listener factory call).

### 5.2 `DataFormatAwareNRTReplicationEngine.java`

Symmetric:
- Replace `index`/`delete`/`noOp` (lines 393-426) with three one-line delegations.
- Replace the `TranslogEventListener` block in the ctor (lines 202-216) with a call to the factory.
- Replace the private `getTranslogDeletionPolicy()` method (lines 899-912) with a one-line delegation:
  ```java
  private TranslogDeletionPolicy getTranslogDeletionPolicy() {
      return NRTReplicaTranslogOps.getTranslogDeletionPolicy(engineConfig);
  }
  ```

Alternative for the last one: inline the call at the single call site (the ctor) and delete the private method entirely. **Preferred** because it removes one more indirection.

### 5.3 `Engine.java`

**Optional, deferred.** `Engine.getTranslogDeletionPolicy(EngineConfig)` (lines 968-981) could delegate to `NRTReplicaTranslogOps.getTranslogDeletionPolicy(engineConfig)` to remove the third copy. This is **deferred** because:

- The method is called by many `Engine` subclasses beyond the two replica engines (`InternalEngine`, `IngestionEngine`, `ReadOnlyEngine`, `NoOpEngine`, etc.).
- Changing it is a widening of blast radius for no additional deduplication benefit (`NRTReplicaTranslogOps` already holds the canonical copy).
- If drift concerns arise later, the one-line delegation is trivial to add.

The doc acknowledges the third copy remains; the name `NRTReplicaTranslogOps` makes it clear this helper is about the **replica-only** engines.

---

## 6. Test plan

### 6.1 New unit tests

`server/src/test/java/org/opensearch/index/engine/NRTReplicaTranslogOpsTests.java`

Six tests:

1. **`testIndexAppendsToTranslogAndAdvancesSeqNo`**
   - Mock `WriteOnlyTranslogManager.add(...)` returns a fake `Translog.Location`.
   - Mock `LocalCheckpointTracker`.
   - Call `NRTReplicaTranslogOps.index(...)`.
   - Verify: returned `Engine.IndexResult` has the passed location, `took` is positive, `advanceMaxSeqNo(seqNo)` was called on tracker, result is frozen.

2. **`testDeleteAppendsToTranslogAndAdvancesSeqNo`** — symmetric.

3. **`testNoOpAppendsToTranslogAndAdvancesSeqNo`** — symmetric.

4. **`testTranslogEventListenerForwardsFailureToFailEngine`**
   - Create listener via factory with a `BiConsumer` spy.
   - Call `listener.onFailure("reason", new RuntimeException("x"))`.
   - Verify spy observed exactly one call with `("reason", the exception)`.

5. **`testTranslogEventListenerTrimsUnreferencedReadersOnSync`**
   - Create listener with a `Supplier` that returns a mock manager.
   - Call `listener.onAfterTranslogSync()`.
   - Verify `mockManager.trimUnreferencedReaders()` was called.

6. **`testTranslogEventListenerWrapsTrimIOExceptionInTranslogException`**
   - Mock manager's `trimUnreferencedReaders()` throws `IOException`.
   - Call `listener.onAfterTranslogSync()`.
   - Expect `TranslogException` with the original `IOException` as cause.

7. **`testGetTranslogDeletionPolicyUsesCustomFactoryWhenPresent`**
   - Build `EngineConfig` with a custom factory stub returning a marker policy.
   - Call `NRTReplicaTranslogOps.getTranslogDeletionPolicy(config)`.
   - Assert the marker policy is returned.

8. **`testGetTranslogDeletionPolicyFallsBackToDefault`**
   - Build `EngineConfig` with no custom factory.
   - Call the method.
   - Assert returned instance is `DefaultTranslogDeletionPolicy` with retention-size/age/total-files matching the config.

### 6.2 Regression verification

Run existing suites — **no modifications expected**:

```
./gradlew :server:test \
  --tests "*NRTReplicationEngineTests*" \
  --tests "*DataFormatAwareEngineTests*" \
  --tests "*IndexShardTests*" \
  --tests "*SegmentReplicationTargetTests*" \
  --tests "*CatalogSnapshotManagerTests*"
./gradlew -Dsandbox.enabled=true :sandbox:plugins:composite-engine:internalClusterTest \
  --tests "*DataFormatAware*IT"
./gradlew -Dsandbox.enabled=true :server:precommit
```

### 6.3 Behavioral equivalence check

Before and after, manually diff the `Translog.Location`, `took`, and seqno-advancement side-effects for a single indexed doc in each engine via a focused unit test (`testReplicaEngineTranslogSideEffectsUnchangedByRefactor`). Optional — the existing test suite implicitly covers this — but useful smoke for confidence.

---

## 7. Rollout

### Phase 1a — new file + tests (independently committable)

1. Create `NRTReplicaTranslogOps.java` (§4.2).
2. Create `NRTReplicaTranslogOpsTests.java` (§6.1).
3. Build and run the new tests. Do not touch the two engines yet.
4. Commit.

Rationale: isolates the helper introduction from the call-site swap. If reviewers push back on helper shape, the follow-up PR just updates this one file.

### Phase 1b — switch call sites

5. Update `NRTReplicationEngine.java` per §5.1.
6. Update `DataFormatAwareNRTReplicationEngine.java` per §5.2 (including deleting the private `getTranslogDeletionPolicy` method).
7. Run full regression suite (§6.2).
8. Commit (separate from 1a if reviewing two commits; amend into 1a if history cleanliness matters).

### Optional Phase 1c — engine-base cleanup

9. Change `Engine.getTranslogDeletionPolicy(EngineConfig)` to delegate to the helper.
10. Run full `:server:test`.
11. Commit.

Defer unless reviewers explicitly ask.

---

## 8. Risks + mitigation

| Risk | Severity | Mitigation |
|---|---|---|
| Refactor accidentally changes the order of `setTranslogLocation` / `setTook` / `freeze` / `advanceMaxSeqNo`. | HIGH | Side-by-side diff the before/after ordering in review. Unit tests 1-3 assert the observable consequence (frozen result, advanced tracker). Integration tests exercise the real flow end-to-end. |
| `Supplier<WriteOnlyTranslogManager>` captures `this` and leaks the engine after close. | LOW | Engines already close the translog manager in `closeNoLock`; the supplier holds no state beyond the reference, GC reclaims once the engine is unreachable. |
| Listener's `BiConsumer<String, Exception>` vs existing `failEngine(String, Exception)` signature mismatch. | LOW | Both engines' `failEngine` is `(String, Exception) -> void`. The method reference `this::failEngine` binds cleanly. Unit test 4 verifies. |
| Package-private prevents future reuse. | LOW | Easy to widen to `public` later. Starting closed matches the project's conservative convention. |
| Test doubles for `EngineConfig` in the policy-factory tests are heavy to set up. | LOW | Reuse `EngineTestCase` helpers for a minimal config; the two paths (custom factory present / absent) are small. |

---

## 9. Expected LOC delta

| File | Lines before | Lines after | Delta |
|---|---|---|---|
| `NRTReplicaTranslogOps.java` | 0 (new) | ~130 (with javadoc) | +130 |
| `NRTReplicaTranslogOpsTests.java` | 0 (new) | ~150 | +150 |
| `NRTReplicationEngine.java` | 620 | ~595 | −25 |
| `DataFormatAwareNRTReplicationEngine.java` | 914 | ~884 | −30 |
| **Totals** | **1534** | **1759** | **+225** |

**Production LOC:** +75 (130 new, 55 removed). **Test LOC:** +150.

The production LOC grows slightly because Java utility-class boilerplate (package/imports/javadoc/class declaration) exceeds the handful of lines saved. **The value is drift prevention, not size reduction.**

---

## 10. Reviewer checklist

- [ ] `NRTReplicaTranslogOps` is package-private and stateless.
- [ ] Three write methods match the current behavior line-for-line (ordering of `setTranslogLocation`/`setTook`/`freeze`/`advanceMaxSeqNo`).
- [ ] `TranslogEventListener` factory preserves the `failEngine` + `trimUnreferencedReaders` + `TranslogException` wrap semantics.
- [ ] `getTranslogDeletionPolicy(EngineConfig)` matches `Engine`'s inherited version.
- [ ] Both engines' ctors still pass the same arguments to `WriteOnlyTranslogManager` (only the listener construction changes).
- [ ] `ensureOpen()` is still the first line of each write method on the engine side.
- [ ] Full regression suite (`:server:precommit` + DFA ITs + targeted unit tests) passes.
- [ ] No test files modified.
