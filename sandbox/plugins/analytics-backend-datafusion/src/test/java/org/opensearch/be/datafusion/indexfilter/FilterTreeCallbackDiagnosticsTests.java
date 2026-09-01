/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.indexfilter;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.spi.FilterDelegationHandle;
import org.opensearch.common.logging.Loggers;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Regression tests for the SILENT {@code -1} returns from {@link FilterTreeCallbacks}.
 *
 * <p>Bug: {@code collectDocs} returned {@code -1} from several separate conditions
 * (no binding / binding closed / cancelled / handle error) with NO diagnostic at all. The native side turns any
 * negative return into
 * {@code "collector.collect_packed_u64_bitset(rg=…): collectDocs(context_id=…, key=…) failed: -1"},
 * so on a real cluster the failure surfaced three layers away with no way to tell WHICH condition
 * fired. Only the {@code catch Throwable} path logged — and that one demonstrably had not fired
 * (zero occurrences of its message in the worker logs).
 *
 * <p>Each test asserts the specific give-up cause is now named in the logs. All of them failed before
 * the fix (no log event was emitted at all) and pass after.
 */
public class FilterTreeCallbackDiagnosticsTests extends OpenSearchTestCase {

    private static final String LOGGER_NAME = FilterTreeCallbacks.class.getCanonicalName();

    private Logger callbackLogger;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        callbackLogger = LogManager.getLogger(FilterTreeCallbacks.class);
        // The cancelled case logs at DEBUG (expected teardown), so the logger must admit DEBUG.
        Loggers.setLevel(callbackLogger, Level.DEBUG);
    }

    /**
     * A contextId is unique per test run so the once-per-(contextId, op, cause) de-duplication in
     * FilterTreeCallbacks never suppresses the event this test is asserting on.
     */
    private static long freshContextId() {
        return CONTEXT_IDS.incrementAndGet();
    }

    private static final AtomicLong CONTEXT_IDS = new AtomicLong(9_000_000L);

    /**
     * THE OBSERVED BUG. A native collector call arrives with no binding for its contextId — either
     * {@code register} never ran, or the binding was force-removed (the test-only
     * {@code unregister}) while a row-group prefetch was still in flight. Before the fix this
     * returned -1 with zero diagnostics; now it names NO BINDING plus the contextId, collector key,
     * and doc range.
     *
     * <p>Runs with assertions disabled semantics in mind: {@code assertBindingExists} would throw
     * under {@code -ea}, so the log call is placed BEFORE it and this test catches the
     * AssertionError to verify the production diagnostic still fired.
     */
    public void testCollectDocsOnUnregisteredContextLogsNoBinding() throws Exception {
        long contextId = freshContextId();
        FilterTreeCallbacks.unregister(contextId);

        try (MockLogAppender appender = MockLogAppender.createForLoggers(callbackLogger)) {
            appender.addExpectation(
                new MockLogAppender.PatternSeenEventExpectation(
                    "collectDocs no-binding is diagnosed",
                    LOGGER_NAME,
                    Level.WARN,
                    ".*collectDocs\\(contextId=" + contextId + ", key=7\\) \\[0, 25\\).*NO BINDING.*"
                )
            );

            try (Arena arena = Arena.ofConfined()) {
                MemorySegment buf = arena.allocate(Long.BYTES);
                // -ea is on in tests, so the (test-only) lifecycle assert fires after the log.
                expectThrows(AssertionError.class, () -> FilterTreeCallbacks.collectDocs(contextId, 7, 0, 25, buf, 1));
            }

            appender.assertAllExpectationsMatched();
        }
    }

    /**
     * A cancelled query's in-flight collector calls are EXPECTED to be refused, so this is
     * logged at DEBUG (not WARN) — it must stay diagnosable without being operator noise.
     */
    public void testCollectDocsOnCancelledQueryLogsAtDebug() throws Exception {
        long contextId = freshContextId();
        AtomicBoolean cancelled = new AtomicBoolean(true);
        FilterTreeCallbacks.register(contextId, new StubHandle(cancelled), null);

        try (MockLogAppender appender = MockLogAppender.createForLoggers(callbackLogger)) {
            appender.addExpectation(
                new MockLogAppender.PatternSeenEventExpectation(
                    "collectDocs cancellation is diagnosed at DEBUG",
                    LOGGER_NAME,
                    Level.DEBUG,
                    ".*collectDocs\\(contextId=" + contextId + ", key=3\\).*query cancelled.*"
                )
            );

            try (Arena arena = Arena.ofConfined()) {
                MemorySegment buf = arena.allocate(Long.BYTES);
                assertEquals(-1L, FilterTreeCallbacks.collectDocs(contextId, 3, 0, 64, buf, 1));
            }

            appender.assertAllExpectationsMatched();
        } finally {
            FilterTreeCallbacks.unregister(contextId);
        }
    }

    /**
     * The accepting backend's handle itself returned a negative result (unknown collector key,
     * or the segment/reader is gone). Previously indistinguishable from the other two.
     */
    public void testCollectDocsHandleErrorLogsHandleError() throws Exception {
        long contextId = freshContextId();
        FilterTreeCallbacks.register(contextId, new StubHandle(new AtomicBoolean(false)), null);

        try (MockLogAppender appender = MockLogAppender.createForLoggers(callbackLogger)) {
            appender.addExpectation(
                new MockLogAppender.PatternSeenEventExpectation(
                    "collectDocs handle error is diagnosed",
                    LOGGER_NAME,
                    Level.WARN,
                    ".*collectDocs\\(contextId=" + contextId + ", key=5\\).*negative result.*"
                )
            );

            try (Arena arena = Arena.ofConfined()) {
                MemorySegment buf = arena.allocate(Long.BYTES);
                // StubHandle.collectDocs always returns -1 when not cancelled.
                assertEquals(-1L, FilterTreeCallbacks.collectDocs(contextId, 5, 0, 64, buf, 1));
            }

            appender.assertAllExpectationsMatched();
        } finally {
            FilterTreeCallbacks.unregister(contextId);
        }
    }

    /**
     * The give-up log is de-duplicated per (contextId, op, cause): {@code collectDocs} is a
     * per-row-group hot callback, and a torn-down query fails EVERY remaining row group. Without
     * dedup the fix would trade a silent failure for a log flood. Asserts the second call on the
     * same (contextId, cause) emits nothing.
     */
    public void testRepeatedGiveUpsAreLoggedOnlyOnce() throws Exception {
        long contextId = freshContextId();
        FilterTreeCallbacks.register(contextId, new StubHandle(new AtomicBoolean(true)), null);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment buf = arena.allocate(Long.BYTES);
            // First call emits the DEBUG diagnostic (asserted by the test above).
            assertEquals(-1L, FilterTreeCallbacks.collectDocs(contextId, 1, 0, 64, buf, 1));

            try (MockLogAppender appender = MockLogAppender.createForLoggers(callbackLogger)) {
                appender.addExpectation(
                    new MockLogAppender.UnseenEventExpectation(
                        "repeat give-up must not re-log",
                        LOGGER_NAME,
                        Level.DEBUG,
                        "*collectDocs(contextId=" + contextId + "*"
                    )
                );
                // Subsequent row groups on the same cancelled query must stay silent.
                for (int i = 0; i < 50; i++) {
                    assertEquals(-1L, FilterTreeCallbacks.collectDocs(contextId, 1, 0, 64, buf, 1));
                }
                appender.assertAllExpectationsMatched();
            }
        } finally {
            FilterTreeCallbacks.unregister(contextId);
        }
    }

    /** createCollector's no-binding path was equally silent; it now names the same cause. */
    public void testCreateCollectorOnUnregisteredContextLogsNoBinding() throws Exception {
        long contextId = freshContextId();
        FilterTreeCallbacks.unregister(contextId);

        try (MockLogAppender appender = MockLogAppender.createForLoggers(callbackLogger)) {
            appender.addExpectation(
                new MockLogAppender.PatternSeenEventExpectation(
                    "createCollector no-binding is diagnosed",
                    LOGGER_NAME,
                    Level.WARN,
                    ".*createCollector\\(contextId=" + contextId + ".*NO BINDING.*"
                )
            );

            expectThrows(AssertionError.class, () -> FilterTreeCallbacks.createCollector(contextId, 1, 2L, 0, 64));

            appender.assertAllExpectationsMatched();
        }
    }

    /**
     * Handle that mimics the production shapes we care about: it can report the owning query
     * cancelled, and its {@code collectDocs} returns {@code -1} otherwise (the handle-error path).
     */
    private static final class StubHandle implements FilterDelegationHandle {
        private final AtomicBoolean cancelled;

        StubHandle(AtomicBoolean cancelled) {
            this.cancelled = cancelled;
        }

        @Override
        public boolean isCancelled() {
            return cancelled.get();
        }

        @Override
        public int createProvider(int annotationId) {
            return 1;
        }

        @Override
        public int createCollector(int providerKey, long writerGeneration, int minDoc, int maxDoc) {
            return 1;
        }

        @Override
        public long collectDocs(int collectorKey, int minDoc, int maxDoc, MemorySegment out) {
            return -1L;
        }

        @Override
        public void releaseCollector(int collectorKey) {}

        @Override
        public void releaseProvider(int providerKey) {}

        @Override
        public void close() {}
    }
}
