/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.index.engine.exec.IndexReaderProvider.Reader;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.mock;

public class ReaderContextTests extends OpenSearchTestCase {

    private GatedCloseable<Reader> mockGatedReader() {
        Reader reader = mock(Reader.class);
        AtomicBoolean closed = new AtomicBoolean(false);
        return new GatedCloseable<>(reader, () -> closed.set(true));
    }

    public void testMarkInUseAndDone() {
        GatedCloseable<Reader> gated = mockGatedReader();
        ReaderContext ctx = new ReaderContext("q1", gated, 30_000);

        assertTrue("Should mark in-use successfully", ctx.markInUse());
        assertFalse("Second markInUse should fail (already in-use)", ctx.markInUse());

        ctx.markDone();
        assertTrue("Should mark in-use again after done", ctx.markInUse());
    }

    public void testNotExpiredWhileInUse() {
        GatedCloseable<Reader> gated = mockGatedReader();
        ReaderContext ctx = new ReaderContext("q1", gated, 1); // 1ms keepAlive

        ctx.markInUse();
        assertFalse("Should not expire while in-use", ctx.isExpired());
    }

    public void testExpiresAfterKeepAlive() throws Exception {
        GatedCloseable<Reader> gated = mockGatedReader();
        ReaderContext ctx = new ReaderContext("q1", gated, 50); // 50ms keepAlive

        ctx.markInUse();
        ctx.markDone();

        Thread.sleep(100);
        assertTrue("Should expire after keepAlive", ctx.isExpired());
    }

    public void testNotExpiredBeforeKeepAlive() {
        GatedCloseable<Reader> gated = mockGatedReader();
        ReaderContext ctx = new ReaderContext("q1", gated, 30_000);

        ctx.markInUse();
        ctx.markDone();

        assertFalse("Should not expire before keepAlive", ctx.isExpired());
    }

    public void testCloseReleasesReader() throws Exception {
        AtomicBoolean closed = new AtomicBoolean(false);
        Reader reader = mock(Reader.class);
        GatedCloseable<Reader> gated = new GatedCloseable<>(reader, () -> closed.set(true));

        ReaderContext ctx = new ReaderContext("q1", gated, 30_000);
        ctx.close();

        assertTrue("Reader should be closed", closed.get());
    }

    public void testMarkInUseAfterCloseReturnsFalse() throws Exception {
        GatedCloseable<Reader> gated = mockGatedReader();
        ReaderContext ctx = new ReaderContext("q1", gated, 30_000);
        ctx.close();

        assertFalse("markInUse should fail after close", ctx.markInUse());
    }

    public void testGetReader() {
        Reader reader = mock(Reader.class);
        GatedCloseable<Reader> gated = new GatedCloseable<>(reader, () -> {});
        ReaderContext ctx = new ReaderContext("q1", gated, 30_000);

        assertSame(reader, ctx.getReader());
    }

    public void testGetQueryId() {
        GatedCloseable<Reader> gated = mockGatedReader();
        ReaderContext ctx = new ReaderContext("test-query-123", gated, 30_000);

        assertEquals("test-query-123", ctx.getQueryId());
    }

    public void testLastAccessTimeUpdatedOnMarkInUse() throws Exception {
        GatedCloseable<Reader> gated = mockGatedReader();
        ReaderContext ctx = new ReaderContext("q1", gated, 30_000);

        long timeAfterCreate = ctx.getLastAccessTime();

        Thread.sleep(20);
        ctx.markInUse();
        long timeAfterMarkInUse = ctx.getLastAccessTime();
        assertTrue("lastAccessTime should advance on markInUse", timeAfterMarkInUse > timeAfterCreate);

        Thread.sleep(20);
        ctx.markDone();
        long timeAfterDone = ctx.getLastAccessTime();
        assertTrue("lastAccessTime should advance on markDone", timeAfterDone > timeAfterMarkInUse);
    }
}
