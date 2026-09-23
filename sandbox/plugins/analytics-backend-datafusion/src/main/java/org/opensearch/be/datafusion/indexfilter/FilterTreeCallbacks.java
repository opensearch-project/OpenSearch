/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.indexfilter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.analytics.spi.DelegationThreadTracker;
import org.opensearch.analytics.spi.FilterDelegationHandle;
import org.opensearch.common.util.concurrent.AbstractRefCounted;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Static callback targets invoked by the native engine via FFM upcalls.
 *
 * <p>Each callback receives a {@code contextId} (the per-query identifier assigned by
 * {@code QueryTrackingContext}) as its first argument, which is used to look up the
 * correct per-query {@link FilterDelegationHandle} and {@link DelegationThreadTracker}
 * from {@link #BINDINGS}. This eliminates the global-singleton race condition that
 * existed when concurrent queries shared a single AtomicReference.
 *
 * <h2>Lifecycle</h2>
 * <ol>
 *   <li>Before query execution: {@link #register(long, FilterDelegationHandle, DelegationThreadTracker)}
 *       installs a binding for the query's contextId.</li>
 *   <li>FFM upcalls route to the correct per-query handle via contextId. Each
 *       successful {@code create*} upcall holds a reference on the binding; the
 *       matching {@code release*} upcall drops it.</li>
 *   <li>After query completion: {@link #requestClose(long)} drops the query's own
 *       reference. The binding is removed — and the handle closed — when the last
 *       reference drops, so late release upcalls from a partially-consumed native
 *       stream (e.g. a satisfied LIMIT, whose final drop runs on a DataFusion
 *       runtime thread after Java-side teardown returns) still find the binding.</li>
 * </ol>
 *
 * <h2>Error-handling contract</h2>
 * <p>Every method catches all {@link Throwable}s and returns {@code -1}
 * (or silently returns for void methods). A Java exception escaping through
 * an FFM upcall stub crashes the JVM.
 *
 * <h2>Lifecycle assertions</h2>
 * <p>When assertions are enabled ({@code -ea}, default in tests and {@code ./gradlew run}),
 * the callbacks {@code assert} that a binding exists before performing the upcall, and
 * {@link #register} asserts no stale binding is left behind. These catch lifecycle bugs
 * (double-register, premature teardown, leaked bindings) during development without
 * affecting production behavior — assertions are off in production, where the same paths
 * fall back to returning -1 silently.
 */
public final class FilterTreeCallbacks {

    private static final Logger LOGGER = LogManager.getLogger(FilterTreeCallbacks.class);

    /**
     * Per-query binding of handle and tracker, keyed by contextId.
     * ConcurrentHashMap provides safe concurrent access across parallel queries.
     */
    private static final ConcurrentHashMap<Long, QueryBinding> BINDINGS = new ConcurrentHashMap<>();

    /**
     * Per-query binding whose lifetime is reference-counted: the query itself holds
     * one reference (dropped by {@link #requestClose}), and every live native handle
     * (provider or collector) holds one ({@code create*} acquires it, {@code release*}
     * drops it — all through the internal {@code refCounter}). The
     * binding is removed — and the delegation handle closed — exactly once, when the
     * last reference drops, which is what keeps late release upcalls from
     * partially-consumed native streams safe.
     */
    private static final class QueryBinding {
        private final long contextId;
        private final FilterDelegationHandle handle;
        private final DelegationThreadTracker tracker;
        /** Ensures {@link #requestClose} drops the query's reference at most once. */
        final AtomicBoolean closeRequested = new AtomicBoolean();
        /**
         * Owns the binding's lifetime: born at 1 (the query's reference), one more per
         * live native handle. Reaching zero closes the binding — {@code closeInternal}
         * links the counter to {@link #close()}.
         */
        private final AbstractRefCounted refCounter = new AbstractRefCounted("filter-delegation-binding") {
            @Override
            protected void closeInternal() {
                QueryBinding.this.close();
            }
        };

        QueryBinding(long contextId, FilterDelegationHandle handle, DelegationThreadTracker tracker) {
            this.contextId = contextId;
            this.handle = java.util.Objects.requireNonNull(handle, "handle");
            this.tracker = tracker;
        }

        DelegationThreadTracker tracker() {
            return tracker;
        }

        /**
         * Create a provider via the handle, acquiring one reference for the native
         * handle on success. Returns -1 (without holding a reference) if the binding
         * is already fully closed or the handle refuses.
         */
        int createProvider(int annotationId) {
            if (refCounter.tryIncRef() == false) {
                assert false : "createProvider: binding already fully closed for contextId=" + contextId;
                return -1;
            }
            boolean created = false;
            try {
                int key = handle.createProvider(annotationId);
                created = key >= 0;
                return key;
            } finally {
                if (created == false) {
                    refCounter.decRef(); // no native handle exists to release this reference later
                }
            }
        }

        /** Collector twin of {@link #createProvider(int)} — same reference contract. */
        int createCollector(int providerKey, long writerGeneration, int minDoc, int maxDoc) {
            if (refCounter.tryIncRef() == false) {
                assert false : "createCollector: binding already fully closed for contextId=" + contextId;
                return -1;
            }
            boolean created = false;
            try {
                int key = handle.createCollector(providerKey, writerGeneration, minDoc, maxDoc);
                created = key >= 0;
                return key;
            } finally {
                if (created == false) {
                    refCounter.decRef();
                }
            }
        }

        /** Release a provider and drop its reference; the last drop closes the binding. */
        void releaseProvider(int providerKey) {
            try {
                handle.releaseProvider(providerKey);
            } finally {
                refCounter.decRef();
            }
        }

        /** Release a collector and drop its reference; the last drop closes the binding. */
        void releaseCollector(int collectorKey) {
            try {
                handle.releaseCollector(collectorKey);
            } finally {
                refCounter.decRef();
            }
        }

        /** Delegate a doc-collection call; refuses (-1) when the query is cancelled. */
        long collectDocs(int collectorKey, int minDoc, int maxDoc, MemorySegment outPtr, long outWordCap) {
            if (handle.isCancelled()) {
                return -1L;
            }
            int maxWords = (int) Math.min(outWordCap, (long) Integer.MAX_VALUE);
            MemorySegment view = outPtr.reinterpret((long) maxWords * Long.BYTES);
            long result = handle.collectDocs(collectorKey, minDoc, maxDoc, view);
            return (result < 0) ? -1L : result;
        }

        /**
         * Drop the query's own reference (at most once). The binding closes — and the
         * delegation handle with it — when the last native handle's reference drops.
         */
        void requestClose() {
            if (closeRequested.compareAndSet(false, true)) {
                refCounter.decRef();
            }
        }

        /** Runs exactly once, when the last reference (query or native handle) drops. */
        private void close() {
            BINDINGS.remove(contextId);
            try {
                handle.close();
            } catch (Throwable throwable) {
                // May run on an FFM upcall thread — must not throw.
                LOGGER.warn(new ParameterizedMessage("FilterDelegationHandle.close() failed for contextId={}", contextId), throwable);
            }
        }
    }

    private FilterTreeCallbacks() {}

    /**
     * Register a per-query binding keyed by {@code contextId}.
     * Must be called before query execution begins.
     *
     * <p>Asserts no prior binding exists for {@code contextId}. A pre-existing binding
     * indicates a leaked binding from an earlier query (missing {@link #requestClose(long)})
     * or a duplicate register call.
     *
     * @param contextId the per-query identifier (from the native {@code QueryTrackingContext})
     * @param handle    the delegation handle for this query (must not be null)
     * @param tracker   the thread tracker for this query (may be null)
     */
    public static void register(long contextId, FilterDelegationHandle handle, DelegationThreadTracker tracker) {
        QueryBinding prev = BINDINGS.put(contextId, new QueryBinding(contextId, handle, tracker));
        assert prev == null : "FilterTreeCallbacks.register: binding already present for contextId=" + contextId;
    }

    /**
     * Request teardown of the binding for {@code contextId}: closes and removes it
     * immediately if no native handles are outstanding, otherwise defers until the
     * last {@code release*} upcall arrives (late drops of partially-consumed native
     * streams happen on DataFusion runtime threads after Java-side teardown returns).
     *
     * <p>Owns {@code handle.close()} — callers must not close the handle themselves.
     * Idempotent — calling with no current binding is a no-op.
     */
    public static void requestClose(long contextId) {
        QueryBinding binding = BINDINGS.get(contextId);
        if (binding == null) {
            return;
        }
        binding.requestClose();
    }

    /**
     * Force-remove the binding without closing the handle. Test-only escape hatch for
     * suites that manage handle lifecycles themselves; production teardown must use
     * {@link #requestClose(long)}.
     */
    // VisibleForTesting
    public static void unregister(long contextId) {
        BINDINGS.remove(contextId);
    }

    private static long trackStart(long contextId) {
        // Must never throw — runs OUTSIDE the try/catch in each upcall target, so any
        // escaping exception (e.g. an `assert false` in TaskResourceTrackingService when
        // the thread is already tracked) crosses the FFM boundary and aborts the JVM
        // with `Unrecoverable uncaught exception encountered`. Swallow everything and
        // disable tracking for the remainder of this upcall by returning -1.
        try {
            QueryBinding binding = BINDINGS.get(contextId);
            if (binding == null) return -1;
            DelegationThreadTracker t = binding.tracker();
            return (t != null) ? t.trackStart() : -1;
        } catch (Throwable throwable) {
            LOGGER.warn("trackStart failed; resource attribution disabled for this upcall", throwable);
            return -1;
        }
    }

    private static void trackEnd(long contextId, long threadId) {
        if (threadId < 0) return;
        // Same FFM safety rule as trackStart — runs in a `finally` block, so any
        // exception escaping here would mask the actual upcall result and abort the JVM.
        try {
            QueryBinding binding = BINDINGS.get(contextId);
            if (binding == null) return;
            DelegationThreadTracker t = binding.tracker();
            if (t != null) t.trackEnd(threadId);
        } catch (Throwable throwable) {
            LOGGER.warn("trackEnd failed", throwable);
        }
    }

    /**
     * Asserts a binding exists. Lifecycle bugs (premature teardown, missing register,
     * stale Rust handle outliving its query) trip this in tests; production silently
     * returns -1 from the caller's null check.
     *
     * <p>Throws {@link AssertionError} when assertions are enabled and binding is null.
     * Upcall methods catch {@code Throwable} and re-throw {@code AssertionError} so it
     * surfaces in tests (causing the JVM to exit through the FFM stub) rather than
     * being silently logged. Under the refcounted lifecycle a missing binding is a
     * genuine bug: bindings stay alive until the last native handle is released.
     */
    private static void assertBindingExists(QueryBinding binding, String op, long contextId) {
        assert binding != null : "FilterTreeCallbacks."
            + op
            + ": no binding for contextId="
            + contextId
            + " (registered: "
            + BINDINGS.keySet()
            + ")";
    }

    // ── Provider lifecycle (cold path, once per query) ────────────────

    /**
     * {@code createProvider(contextId, annotationId) -> providerKey|-1}.
     */
    public static int createProvider(long contextId, int annotationId) {
        long tid = trackStart(contextId);
        try {
            QueryBinding binding = BINDINGS.get(contextId);
            assertBindingExists(binding, "createProvider", contextId);
            if (binding == null) {
                return -1;
            }
            return binding.createProvider(annotationId);
        } catch (AssertionError e) {
            // Propagate so lifecycle bugs surface in tests; in production -ea is off and this branch never runs.
            throw e;
        } catch (Throwable throwable) {
            LOGGER.error("createProvider failed for contextId=" + contextId + " annotationId=" + annotationId, throwable);
            return -1;
        } finally {
            trackEnd(contextId, tid);
        }
    }

    /**
     * {@code releaseProvider(contextId, providerKey)}. Never throws.
     */
    public static void releaseProvider(long contextId, int providerKey) {
        try {
            QueryBinding binding = BINDINGS.get(contextId);
            assertBindingExists(binding, "releaseProvider", contextId);
            if (binding != null) {
                binding.releaseProvider(providerKey);
            }
        } catch (AssertionError e) {
            throw e;
        } catch (Throwable throwable) {
            LOGGER.error(
                new ParameterizedMessage("releaseProvider(contextId={}, providerKey={}) failed", contextId, providerKey),
                throwable
            );
        }
    }

    // ── Collector lifecycle (hot path, per segment per query) ─────────

    /**
     * {@code createCollector(contextId, providerKey, writerGeneration, minDoc, maxDoc) -> collectorKey|-1}.
     *
     * <p>Segments are identified by writer generation.
     */
    public static int createCollector(long contextId, int providerKey, long writerGeneration, int minDoc, int maxDoc) {
        long tid = trackStart(contextId);
        try {
            QueryBinding binding = BINDINGS.get(contextId);
            assertBindingExists(binding, "createCollector", contextId);
            if (binding == null) {
                return -1;
            }
            return binding.createCollector(providerKey, writerGeneration, minDoc, maxDoc);
        } catch (AssertionError e) {
            throw e;
        } catch (Throwable throwable) {
            LOGGER.error(
                new ParameterizedMessage(
                    "createCollector(contextId={}, providerKey={}, writerGeneration={}, [{}, {})) failed",
                    contextId,
                    providerKey,
                    writerGeneration,
                    minDoc,
                    maxDoc
                ),
                throwable
            );
            return -1;
        } finally {
            trackEnd(contextId, tid);
        }
    }

    /**
     * {@code collectDocs(contextId, collectorKey, minDoc, maxDoc, outPtr, outWordCap) -> packed(nextDoc|wordsWritten)|-1}.
     */
    public static long collectDocs(long contextId, int collectorKey, int minDoc, int maxDoc, MemorySegment outPtr, long outWordCap) {
        long tid = trackStart(contextId);
        try {
            QueryBinding binding = BINDINGS.get(contextId);
            assertBindingExists(binding, "collectDocs", contextId);
            if (binding == null) {
                return -1L;
            }
            return binding.collectDocs(collectorKey, minDoc, maxDoc, outPtr, outWordCap);
        } catch (AssertionError e) {
            throw e;
        } catch (Throwable throwable) {
            LOGGER.error(
                new ParameterizedMessage(
                    "collectDocs(contextId={}, collectorKey={}, [{}, {})) failed",
                    contextId,
                    collectorKey,
                    minDoc,
                    maxDoc
                ),
                throwable
            );
            return -1L;
        } finally {
            trackEnd(contextId, tid);
        }
    }

    /**
     * {@code releaseCollector(contextId, collectorKey)}. Never throws.
     */
    public static void releaseCollector(long contextId, int collectorKey) {
        try {
            QueryBinding binding = BINDINGS.get(contextId);
            assertBindingExists(binding, "releaseCollector", contextId);
            if (binding != null) {
                binding.releaseCollector(collectorKey);
            }
        } catch (AssertionError e) {
            throw e;
        } catch (Throwable throwable) {
            LOGGER.error(
                new ParameterizedMessage("releaseCollector(contextId={}, collectorKey={}) failed", contextId, collectorKey),
                throwable
            );
        }
    }
}
