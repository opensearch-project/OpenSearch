/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.test.OpenSearchTestCase;

public class NativeErrorConverterTests extends OpenSearchTestCase {

    public void testPoolLimitExceededConvertsToCircuitBreakingException() {
        String message = "Failed to allocate 1048576 bytes for hash_agg (524288 already reserved) "
            + "— 0 available out of 4294967296 limit";
        RuntimeException original = new RuntimeException(message);

        Exception result = NativeErrorConverter.convert(original);

        assertNotNull(result);
        assertTrue(result instanceof CircuitBreakingException);
        CircuitBreakingException cbe = (CircuitBreakingException) result;
        assertEquals(1048576L, cbe.getBytesWanted());
        assertEquals(4294967296L, cbe.getByteLimit());
        assertEquals(CircuitBreaker.Durability.TRANSIENT, cbe.getDurability());
        assertEquals("[analytics_backend_datafusion] Failed to allocate 1048576 bytes (limit: 4294967296)", cbe.getMessage());
        // The raw native error (with allocator internals) must NOT be attached as the user-facing cause.
        assertNull("converted exception must carry no cause (raw native detail stays server-side)", cbe.getCause());
        assertNoNativeLeak(cbe);
    }

    /** Asserts the rendered exception (message + cause chain) carries no raw native allocator internals. */
    private static void assertNoNativeLeak(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (Throwable c = t; c != null && c != c.getCause(); c = c.getCause()) {
            sb.append(c.getClass().getName()).append(':').append(c.getMessage()).append('\n');
        }
        String rendered = sb.toString();
        for (String leak : new String[] {
            "top memory consumers",
            "query_untracked",
            "can spill",
            "already reserved",
            "GroupedHashAggregateStream",
            "RepartitionExec",
            "batch_size",
            "avg_row_bytes" }) {
            assertFalse("must not leak native detail '" + leak + "' to the user, got: " + rendered, rendered.contains(leak));
        }
    }

    public void testPoolLimitExceededUsesControlledMessage() {
        String message = "Failed to allocate 2048 bytes for sort (1024 already reserved) " + "— 0 available out of 1073741824 limit";
        RuntimeException original = new RuntimeException(message);

        Exception result = NativeErrorConverter.convert(original);

        assertTrue(result instanceof CircuitBreakingException);
        assertEquals("[analytics_backend_datafusion] Failed to allocate 2048 bytes (limit: 1073741824)", result.getMessage());
    }

    public void testAdmissionRejectionConvertsToStatusException() {
        String message = "Cannot reserve untracked memory budget: 67108864 bytes required "
            + "at minimum parallelism (partitions=1, batch_size=1024). Pool capacity exhausted.";
        RuntimeException original = new RuntimeException(message);

        Exception result = NativeErrorConverter.convert(original);

        assertNotNull(result);
        assertTrue(result instanceof OpenSearchStatusException);
        OpenSearchStatusException statusEx = (OpenSearchStatusException) result;
        assertEquals(RestStatus.TOO_MANY_REQUESTS, statusEx.status());
        assertEquals("Native query admission rejected: insufficient memory budget available", result.getMessage());
        assertNull("converted exception must carry no cause (raw native detail stays server-side)", result.getCause());
        assertNoNativeLeak(result);
    }

    public void testCriticalPressureCancellationConvertsToCircuitBreakingException() {
        String message = "Failed to allocate 65536 bytes for hash_agg (1048576 already reserved) "
            + "— 0 available out of 4294967296 limit. "
            + "Query cancelled: native memory RSS exceeds critical threshold (95% of pool limit).";
        RuntimeException original = new RuntimeException(message);

        Exception result = NativeErrorConverter.convert(original);

        assertNotNull(result);
        assertTrue(result instanceof CircuitBreakingException);
        CircuitBreakingException cbe = (CircuitBreakingException) result;
        assertEquals(65536L, cbe.getBytesWanted());
        assertEquals(4294967296L, cbe.getByteLimit());
        assertEquals(CircuitBreaker.Durability.TRANSIENT, cbe.getDurability());
        assertNull("converted exception must carry no cause (raw native detail stays server-side)", cbe.getCause());
        assertNoNativeLeak(cbe);
    }

    public void testSpillPoolExhaustedConvertsToCircuitBreakingException() {
        // DataFusion emits this when an operator can't allocate and DiskManager is disabled —
        // happens with very small datafusion.memory_pool_limit_bytes before our own try_grow path runs.
        String message = "Resources exhausted: Memory Exhausted while SpillPool (DiskManager is disabled)";
        RuntimeException original = new RuntimeException(message);

        Exception result = NativeErrorConverter.convert(original);

        assertTrue(result instanceof CircuitBreakingException);
        CircuitBreakingException cbe = (CircuitBreakingException) result;
        assertEquals(CircuitBreaker.Durability.TRANSIENT, cbe.getDurability());
        assertNull("converted exception must carry no cause (raw native detail stays server-side)", cbe.getCause());
        assertNoNativeLeak(cbe);
    }

    /**
     * The real staging TopK message: a controlled "[analytics_backend_datafusion] Failed to allocate N bytes
     * (limit: L)" with the verbose native allocator dump appended. The converted exception must keep the clean
     * numeric message and NOT echo the consumer breakdown anywhere in its rendered chain.
     */
    public void testTopKConsumerDumpIsSanitized() {
        String message = "[analytics_backend_datafusion] Failed to allocate 307848 bytes (limit: 27673548029)\n"
            + "Execution error: Resources exhausted: Additional allocation failed for TopK[0] with top memory consumers "
            + "(across reservations) as:\n  query_untracked(partitions=4,batch=8192)#49492(can spill: true) consumed 20.7 MB, "
            + "peak 20.7 MB.\nError: Failed to allocate 307848 bytes for TopK[0] (0 already reserved) "
            + "— 0 available out of 27673548029 limit";
        RuntimeException original = new RuntimeException(message);

        Exception result = NativeErrorConverter.convert(original);

        assertTrue(result instanceof CircuitBreakingException);
        assertEquals("[analytics_backend_datafusion] Failed to allocate 307848 bytes (limit: 27673548029)", result.getMessage());
        assertNoNativeLeak(result);
    }

    public void testRawRecursionLimitConvertsToIllegalArgumentException() {
        // Raw prost decoder error seen at the FFM boundary for deeply nested Substrait plans.
        String message = "failed to decode Protobuf message: recursion limit reached";
        RuntimeException original = new RuntimeException(message);

        Exception result = NativeErrorConverter.convert(original);

        assertTrue(result instanceof IllegalArgumentException);
        assertTrue(result.getMessage().contains("too deeply nested"));
        // Cause intentionally omitted to prevent leaking native error details in REST responses.
        assertNull(result.getCause());
    }

    public void testControlledRecursionMessageConvertsOnCoordinator() {
        // Coordinator-side safety net: the controlled message arriving via StreamException.
        String controlledMessage = "Query too deeply nested: the expression exceeds the maximum nesting depth "
            + "supported by the execution engine. Simplify the query by reducing nested function calls.";
        RuntimeException transportException = new RuntimeException(controlledMessage);

        Exception result = NativeErrorConverter.convert(transportException);

        assertTrue(result instanceof IllegalArgumentException);
        assertTrue(result.getMessage().contains("too deeply nested"));
    }

    public void testUnrecognizedErrorPassedThrough() {
        RuntimeException original = new RuntimeException("Some unknown error from native code");

        Exception result = NativeErrorConverter.convert(original);

        assertSame(original, result);
    }

    public void testAlreadyConvertedCircuitBreakingExceptionNotReConverted() {
        CircuitBreakingException cbe = new CircuitBreakingException("already converted", 100, 200, CircuitBreaker.Durability.TRANSIENT);

        Exception result = NativeErrorConverter.convert(cbe);

        assertSame(cbe, result);
    }

    public void testAlreadyConvertedStatusExceptionNotReConverted() {
        OpenSearchStatusException statusEx = new OpenSearchStatusException("already rejected", RestStatus.TOO_MANY_REQUESTS);

        Exception result = NativeErrorConverter.convert(statusEx);

        assertSame(statusEx, result);
    }

    public void testNestedCauseChainIsWalked() {
        String poolMessage = "Failed to allocate 4096 bytes for coalesce (2048 already reserved) — 0 available out of 8589934592 limit";
        RuntimeException inner = new RuntimeException(poolMessage);
        RuntimeException wrapper = new RuntimeException("Query execution failed", inner);

        Exception result = NativeErrorConverter.convert(wrapper);

        assertTrue(result instanceof CircuitBreakingException);
        assertEquals(4096L, ((CircuitBreakingException) result).getBytesWanted());
    }

    public void testDeeplyNestedCauseRespectDepthLimit() {
        // Build a chain deeper than 10
        RuntimeException deepest = new RuntimeException(
            "Failed to allocate 512 bytes for agg (256 already reserved) — 0 available out of 1024 limit"
        );
        Exception current = deepest;
        for (int i = 0; i < 15; i++) {
            current = new RuntimeException("wrapper " + i, current);
        }

        // The pattern is at depth > 10, so it should NOT be matched
        Exception result = NativeErrorConverter.convert((Exception) current);
        assertSame(current, result);
    }

    public void testMalformedPoolLimitMessageFallsThrough() {
        // Contains the key phrase but wrong format
        String message = "Failed to allocate lots of bytes for something";
        RuntimeException original = new RuntimeException(message);

        Exception result = NativeErrorConverter.convert(original);

        // Key phrase matched but regex didn't parse → returns original
        assertSame(original, result);
    }

    public void testNullMessageInCauseChainHandledGracefully() {
        RuntimeException withNullMsg = new RuntimeException((String) null);
        String poolMessage = "Failed to allocate 1024 bytes for test (512 already reserved) — 0 available out of 2048 limit";
        RuntimeException inner = new RuntimeException(poolMessage);
        RuntimeException chain = new RuntimeException(null, new RuntimeException(null, inner));

        Exception result = NativeErrorConverter.convert(chain);

        assertTrue(result instanceof CircuitBreakingException);
    }

    public void testControlledAdmissionMessageConvertsOnCoordinator() {
        // Simulates what the coordinator sees after Flight transport: StreamException wrapping
        // the controlled message produced by the data node's NativeErrorConverter.
        String controlledMessage = "Native query admission rejected: insufficient memory budget available";
        RuntimeException transportException = new RuntimeException(controlledMessage);

        Exception result = NativeErrorConverter.convert(transportException);

        assertTrue(result instanceof OpenSearchStatusException);
        assertEquals(RestStatus.TOO_MANY_REQUESTS, ((OpenSearchStatusException) result).status());
        assertEquals(controlledMessage, result.getMessage());
    }

    public void testControlledPoolLimitMessageConvertsOnCoordinator() {
        // Simulates what the coordinator sees after Flight transport: the controlled
        // pool limit message produced by the data node's NativeErrorConverter.
        String controlledMessage = "[analytics_backend_datafusion] Failed to allocate 1048576 bytes (limit: 4294967296)";
        RuntimeException transportException = new RuntimeException(controlledMessage);

        Exception result = NativeErrorConverter.convert(transportException);

        assertTrue(result instanceof CircuitBreakingException);
        CircuitBreakingException cbe = (CircuitBreakingException) result;
        assertEquals(1048576L, cbe.getBytesWanted());
        assertEquals(4294967296L, cbe.getByteLimit());
    }
}
