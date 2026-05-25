/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
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
        assertSame(original, cbe.getCause());
    }

    public void testPoolLimitExceededWithNativeRequestPrefix() {
        String message = "[analytics_backend_datafusion] Failed to allocate 2048 bytes for sort (1024 already reserved) "
            + "— 0 available out of 1073741824 limit";
        RuntimeException original = new RuntimeException(message);

        Exception result = NativeErrorConverter.convert(original);

        assertTrue(result instanceof CircuitBreakingException);
        assertTrue(result.getMessage().contains("[analytics_backend_datafusion]"));
    }

    public void testAdmissionRejectionConvertsToRejectedExecution() {
        String message = "Cannot reserve untracked memory budget: 67108864 bytes required "
            + "at minimum parallelism (partitions=1, batch_size=1024). Pool capacity exhausted.";
        RuntimeException original = new RuntimeException(message);

        Exception result = NativeErrorConverter.convert(original);

        assertNotNull(result);
        assertTrue(result instanceof OpenSearchRejectedExecutionException);
        assertTrue(result.getMessage().contains("Native query admission rejected"));
        assertTrue(((OpenSearchRejectedExecutionException) result).isExecutorShutdown());
        assertSame(original, result.getCause());
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

    public void testAlreadyConvertedRejectedExecutionNotReConverted() {
        OpenSearchRejectedExecutionException rejection = new OpenSearchRejectedExecutionException("already rejected", true);

        Exception result = NativeErrorConverter.convert(rejection);

        assertSame(rejection, result);
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
}
