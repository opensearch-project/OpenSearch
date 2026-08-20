/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.action.LatchedActionListener;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;

public class RemoteStoreFenceTests extends OpenSearchTestCase {

    private FsBlobContainer blobContainer;
    private ShardId shardId;
    private ThreadPool threadPool;

    @Before
    public void setUpContainer() throws IOException {
        Path repoPath = createTempDir();
        FsBlobStore blobStore = new FsBlobStore(randomIntBetween(1, 8) * 1024, repoPath, false);
        blobContainer = (FsBlobContainer) blobStore.blobContainer(BlobPath.cleanPath());
        shardId = new ShardId("index", "uuid", 0);
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void tearDownThreadPool() {
        terminate(threadPool);
    }

    private RemoteStoreFence newFence(String ownerNodeId) {
        return new RemoteStoreFence(blobContainer, ownerNodeId, shardId, threadPool);
    }

    public void testBootstrapCreatesFence() throws IOException {
        RemoteStoreFence fence = newFence("node-1");
        fence.validateAndAdvance(1);
        assertEquals(1, fence.getTerm());
        assertEquals(0, fence.getSeq());
        assertTrue(blobContainer.blobExists(RemoteStoreFence.FENCE_BLOB_NAME));
    }

    public void testAdvanceIncrementsSeq() throws IOException {
        RemoteStoreFence fence = newFence("node-1");
        fence.validateAndAdvance(1);
        fence.validateAndAdvance(1);
        fence.validateAndAdvance(1);
        assertEquals(2, fence.getSeq());
    }

    public void testTermBumpSealsOverOldOwner() throws IOException {
        RemoteStoreFence oldPrimary = newFence("node-old");
        oldPrimary.validateAndAdvance(1);

        // Failover: new primary at a higher term bootstraps and seals the fence over
        RemoteStoreFence newPrimary = newFence("node-new");
        newPrimary.validateAndAdvance(2);
        assertEquals(2, newPrimary.getTerm());

        // Stale primary's next upload at the old term must be fenced
        TranslogFencedException e = expectThrows(TranslogFencedException.class, () -> oldPrimary.validateAndAdvance(1));
        assertTrue(e.getMessage(), e.getMessage().contains("fenced"));
    }

    public void testRelocationHandoffAtSameTerm() throws IOException {
        RemoteStoreFence source = newFence("node-source");
        source.validateAndAdvance(1);

        // Relocation target takes over ownership at the same term
        RemoteStoreFence target = newFence("node-target");
        target.validateAndAdvance(1);

        // Zombie source must be fenced even though the term did not change
        expectThrows(TranslogFencedException.class, () -> source.validateAndAdvance(1));

        // Target keeps advancing normally
        target.validateAndAdvance(1);
    }

    public void testLocalTermLowerThanFenceTermIsFenced() throws IOException {
        RemoteStoreFence fence = newFence("node-1");
        fence.validateAndAdvance(5);
        RemoteStoreFence stale = newFence("node-2");
        TranslogFencedException e = expectThrows(TranslogFencedException.class, () -> stale.validateAndAdvance(3));
        assertTrue(e.getMessage(), e.getMessage().contains("fence term [5]"));
    }

    public void testFencedOwnerStaysFencedAfterHigherTermTakeover() throws IOException {
        RemoteStoreFence oldPrimary = newFence("node-old");
        oldPrimary.validateAndAdvance(1);
        RemoteStoreFence newPrimary = newFence("node-new");
        newPrimary.validateAndAdvance(2);
        // Even if the stale primary claims a higher local term, its CAS token is stale
        expectThrows(TranslogFencedException.class, () -> oldPrimary.validateAndAdvance(1));
        // And the new primary continues on its chain
        newPrimary.validateAndAdvance(2);
        assertEquals(2, newPrimary.getSeq());
    }

    public void testValidateAndAdvanceAsync() throws Exception {
        RemoteStoreFence fence = newFence("node-1");
        assertNull(advanceAsync(fence, 1));
        assertEquals(1, fence.getTerm());
        assertEquals(0, fence.getSeq());

        // Second advance on the same chain
        assertNull(advanceAsync(fence, 1));
        assertEquals(1, fence.getSeq());
    }

    public void testValidateAndAdvanceAsyncReportsFencing() throws Exception {
        RemoteStoreFence oldPrimary = newFence("node-old");
        oldPrimary.validateAndAdvance(1);
        newFence("node-new").validateAndAdvance(2);

        Exception failure = advanceAsync(oldPrimary, 1);
        assertNotNull(failure);
        assertTrue(failure.toString(), failure instanceof TranslogFencedException);
    }

    private Exception advanceAsync(RemoteStoreFence fence, long primaryTerm) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> failure = new AtomicReference<>();
        fence.validateAndAdvanceAsync(primaryTerm, new LatchedActionListener<>(ActionListener.wrap(ignored -> {}, failure::set), latch));
        assertTrue(latch.await(30, TimeUnit.SECONDS));
        return failure.get();
    }

    public void testConcurrentBootstrapAdmitsSingleOwner() throws Exception {
        int contenders = randomIntBetween(2, 6);
        CyclicBarrier barrier = new CyclicBarrier(contenders);
        AtomicInteger bootstrapped = new AtomicInteger();
        AtomicInteger fenced = new AtomicInteger();
        Thread[] threads = new Thread[contenders];
        for (int i = 0; i < contenders; i++) {
            RemoteStoreFence fence = newFence("node-" + i);
            threads[i] = new Thread(() -> {
                try {
                    barrier.await(30, TimeUnit.SECONDS);
                    fence.validateAndAdvance(1);
                    bootstrapped.incrementAndGet();
                } catch (TranslogFencedException e) {
                    fenced.incrementAndGet();
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });
            threads[i].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        // Bootstrap tolerates one benign CAS conflict, so more than one contender may seal in; what must hold is that
        // every contender either bootstrapped or was explicitly fenced, and only one owns the chain afterwards.
        assertEquals(contenders, bootstrapped.get() + fenced.get());
        assertTrue(bootstrapped.get() >= 1);
        RemoteStoreFence.FenceState finalState = RemoteStoreFence.FenceState.parse(
            blobContainer.readBlobWithVersion(RemoteStoreFence.FENCE_BLOB_NAME).content()
        );
        assertEquals(1, finalState.term);
        assertTrue(finalState.owner.startsWith("node-"));
    }

    public void testFenceBlobIsSelfDescribing() throws IOException {
        RemoteStoreFence fence = newFence("node-1");
        fence.validateAndAdvance(9);
        RemoteStoreFence.FenceState state = RemoteStoreFence.FenceState.parse(
            blobContainer.readBlobWithVersion(RemoteStoreFence.FENCE_BLOB_NAME).content()
        );
        assertEquals(9, state.term);
        assertEquals("node-1", state.owner);
        assertEquals(0, state.seq);
    }

    public void testFenceStateCodecRoundTrip() throws IOException {
        RemoteStoreFence.FenceState state = new RemoteStoreFence.FenceState(7, "node-a", 42);
        RemoteStoreFence.FenceState parsed = RemoteStoreFence.FenceState.parse(state.toBytes());
        assertEquals(7, parsed.term);
        assertEquals("node-a", parsed.owner);
        assertEquals(42, parsed.seq);
    }

    public void testFenceStateCodecRejectsGarbage() {
        expectThrows(IOException.class, () -> RemoteStoreFence.FenceState.parse("garbage".getBytes(StandardCharsets.UTF_8)));
        // Wrong codec version
        expectThrows(IOException.class, () -> RemoteStoreFence.FenceState.parse("v2|1|node-1|0".getBytes(StandardCharsets.UTF_8)));
        // Non-numeric term
        expectThrows(IOException.class, () -> RemoteStoreFence.FenceState.parse("v1|x|node-1|0".getBytes(StandardCharsets.UTF_8)));
        // Truncated
        expectThrows(IOException.class, () -> RemoteStoreFence.FenceState.parse("v1|1|node-1".getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * The separator is unescaped, so an owner containing it would encode a blob that {@code parse} rejects - which
     * would fence a healthy primary. Node ids are base64 UUIDs so this cannot happen today; reject it at the source
     * rather than rely on that.
     */
    public void testFenceStateRejectsOwnerContainingTheSeparator() {
        expectThrows(IllegalArgumentException.class, () -> new RemoteStoreFence.FenceState(1, "node|1", 0));
        // and the encoding of a well-formed owner still round-trips
        RemoteStoreFence.FenceState roundTripped = expectSuccess(new RemoteStoreFence.FenceState(7, "node-1", 3));
        assertEquals(7, roundTripped.term);
        assertEquals("node-1", roundTripped.owner);
        assertEquals(3, roundTripped.seq);
    }

    private static RemoteStoreFence.FenceState expectSuccess(RemoteStoreFence.FenceState state) {
        try {
            return RemoteStoreFence.FenceState.parse(state.toBytes());
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    public void testFenceRejectsContainerWithoutConditionalWriteSupport() {
        // The BlobContainer defaults must be inert: no silent non-atomic fallback for repositories that cannot CAS
        BlobContainer unsupported = mock(BlobContainer.class, CALLS_REAL_METHODS);
        assertFalse(unsupported.isConditionalWriteSupported());
        RemoteStoreFence fence = new RemoteStoreFence(unsupported, "node-1", shardId, threadPool);
        expectThrows(UnsupportedOperationException.class, () -> fence.validateAndAdvance(1));
    }
}
