/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.concurrent;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class GatedConditionalCloseableTests extends OpenSearchTestCase {

    public void testGet() {
        GatedConditionalCloseable<String> handle = new GatedConditionalCloseable<>("value", () -> {}, () -> {});
        assertEquals("value", handle.get());
    }

    public void testCloseWithoutMarkSuccessRunsOnFailure() throws IOException {
        AtomicBoolean successRan = new AtomicBoolean(false);
        AtomicBoolean failureRan = new AtomicBoolean(false);

        GatedConditionalCloseable<String> handle = new GatedConditionalCloseable<>(
            "v",
            () -> successRan.set(true),
            () -> failureRan.set(true)
        );
        handle.close();

        assertFalse(successRan.get());
        assertTrue(failureRan.get());
    }

    public void testCloseAfterMarkSuccessRunsOnSuccess() throws IOException {
        AtomicBoolean successRan = new AtomicBoolean(false);
        AtomicBoolean failureRan = new AtomicBoolean(false);

        GatedConditionalCloseable<String> handle = new GatedConditionalCloseable<>(
            "v",
            () -> successRan.set(true),
            () -> failureRan.set(true)
        );
        handle.markSuccess();
        handle.close();

        assertTrue(successRan.get());
        assertFalse(failureRan.get());
    }

    public void testIdempotentClose() throws IOException {
        AtomicBoolean failureRan = new AtomicBoolean(false);
        int[] count = { 0 };

        GatedConditionalCloseable<String> handle = new GatedConditionalCloseable<>("v", () -> {}, () -> count[0]++);
        handle.close();
        handle.close();
        handle.close();

        assertEquals(1, count[0]);
    }

    public void testOnSuccessException() {
        GatedConditionalCloseable<String> handle = new GatedConditionalCloseable<>("v", () -> { throw new IOException("boom"); }, () -> {});
        handle.markSuccess();
        assertThrows(IOException.class, handle::close);
    }

    public void testOnFailureException() {
        GatedConditionalCloseable<String> handle = new GatedConditionalCloseable<>("v", () -> {}, () -> { throw new IOException("boom"); });
        assertThrows(IOException.class, handle::close);
    }
}
