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

public class GatedBiCloseableTests extends OpenSearchTestCase {

    public void testGet() {
        GatedBiCloseable<String> handle = new GatedBiCloseable<>("value", () -> {}, () -> {});
        assertEquals("value", handle.get());
    }

    public void testCloseWithoutMarkSuccessRunsOnFailure() throws IOException {
        AtomicBoolean successRan = new AtomicBoolean(false);
        AtomicBoolean failureRan = new AtomicBoolean(false);

        GatedBiCloseable<String> handle = new GatedBiCloseable<>("v", () -> successRan.set(true), () -> failureRan.set(true));
        handle.close();

        assertFalse(successRan.get());
        assertTrue(failureRan.get());
    }

    public void testCloseAfterMarkSuccessRunsOnSuccess() throws IOException {
        AtomicBoolean successRan = new AtomicBoolean(false);
        AtomicBoolean failureRan = new AtomicBoolean(false);

        GatedBiCloseable<String> handle = new GatedBiCloseable<>("v", () -> successRan.set(true), () -> failureRan.set(true));
        handle.markSuccess();
        handle.close();

        assertTrue(successRan.get());
        assertFalse(failureRan.get());
    }

    public void testIdempotentClose() throws IOException {
        AtomicBoolean failureRan = new AtomicBoolean(false);
        int[] count = { 0 };

        GatedBiCloseable<String> handle = new GatedBiCloseable<>("v", () -> {}, () -> count[0]++);
        handle.close();
        handle.close();
        handle.close();

        assertEquals(1, count[0]);
    }

    public void testOnSuccessException() {
        GatedBiCloseable<String> handle = new GatedBiCloseable<>("v", () -> { throw new IOException("boom"); }, () -> {});
        handle.markSuccess();
        assertThrows(IOException.class, handle::close);
    }

    public void testOnFailureException() {
        GatedBiCloseable<String> handle = new GatedBiCloseable<>("v", () -> {}, () -> { throw new IOException("boom"); });
        assertThrows(IOException.class, handle::close);
    }
}
