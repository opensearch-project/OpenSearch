/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.allocator;

import org.apache.arrow.memory.OutOfMemoryException;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.test.OpenSearchTestCase;

public class AllocationRejectionTests extends OpenSearchTestCase {

    public void testWrapPassesThroughOnSuccess() {
        String result = AllocationRejection.wrap("ctx", () -> "ok");
        assertEquals("ok", result);
    }

    public void testWrapTranslatesArrowOomToRejection() {
        OutOfMemoryException arrowOom = new OutOfMemoryException(
            "Unable to allocate buffer of size 1024 due to memory limit. Current allocation: 0"
        );
        OpenSearchRejectedExecutionException rejection = expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> AllocationRejection.wrap("query-pool", () -> {
                throw arrowOom;
            })
        );
        assertTrue("rejection message must include the context label", rejection.getMessage().contains("[query-pool]"));
        assertTrue(
            "rejection message must propagate Arrow's diagnostic detail",
            rejection.getMessage().contains("Unable to allocate buffer of size 1024")
        );
        assertSame("Arrow OOM must be attached as cause", arrowOom, rejection.getCause());
        assertFalse(
            "rejection should not be marked as executor-shutdown — this is per-request, not lifecycle",
            rejection.isExecutorShutdown()
        );
    }

    public void testWrapDoesNotInterceptOtherExceptions() {
        // Non-Arrow exceptions must propagate unchanged. The wrapper is OOM-specific.
        IllegalStateException ise = new IllegalStateException("something else");
        IllegalStateException caught = expectThrows(
            IllegalStateException.class,
            () -> AllocationRejection.wrap("ctx", () -> { throw ise; })
        );
        assertSame(ise, caught);
    }

    public void testRunnableOverloadAlsoTranslatesOom() {
        OutOfMemoryException arrowOom = new OutOfMemoryException("limit exceeded");
        OpenSearchRejectedExecutionException rejection = expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> AllocationRejection.wrap("ingest-vsr", (Runnable) () -> {
                throw arrowOom;
            })
        );
        assertTrue(rejection.getMessage().contains("[ingest-vsr]"));
        assertSame(arrowOom, rejection.getCause());
    }
}
