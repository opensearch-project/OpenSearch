/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;

public class RemoteStoreFenceTests extends OpenSearchTestCase {

    private FsBlobContainer blobContainer;
    private FsBlobStore blobStore;
    private Path repoPath;
    private ShardId shardId;
    private ThreadPool threadPool;

    @Before
    public void setUpContainer() throws IOException {
        repoPath = createTempDir();
        blobStore = new FsBlobStore(randomIntBetween(1, 8) * 1024, repoPath, false);
        blobContainer = (FsBlobContainer) blobStore.blobContainer(BlobPath.cleanPath());
        shardId = new ShardId("index", "uuid", 0);
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void tearDownThreadPool() {
        terminate(threadPool);
    }

    /** Allocation ids in these tests are derived from the node id so both identities stay legible in assertions. */
    private static String allocationIdOf(String ownerNodeId) {
        return ownerNodeId + "-alloc";
    }

    private RemoteStoreFence newFence(String ownerNodeId) {
        return new RemoteStoreFence(blobContainer, allocationIdOf(ownerNodeId), ownerNodeId, shardId);
    }

    /**
     * A conditional write whose response is lost must not fence a healthy primary.
     * <p>
     * The write lands, the response does not, so this copy still holds the previous token. Retrying would fail its
     * If-Match for exactly the same reason a genuinely fenced writer's would, and the copy would fail its shard on a
     * network blip. The ambiguity is resolved by identity: the object records the writing copy and its seq, so a blob
     * carrying ours at the seq we attempted can only be the write we just issued.
     */
    public void testLostResponseOnTheAckCasDoesNotFenceAHealthyPrimary() throws IOException {
        final long term = 7;
        final AtomicBoolean swallowNextResponse = new AtomicBoolean();

        final FsBlobContainer losingResponses = new FsBlobContainer(blobStore, BlobPath.cleanPath(), repoPath) {
            @Override
            public String writeBlobConditionally(String blobName, java.io.InputStream in, long size, String expectedToken)
                throws IOException {
                final String token = super.writeBlobConditionally(blobName, in, size, expectedToken);
                if (swallowNextResponse.compareAndSet(true, false)) {
                    // Landed server-side; the caller never learns the new token.
                    throw new IOException("connection reset before the response was read");
                }
                return token;
            }
        };

        final RemoteStoreFence fence = new RemoteStoreFence(losingResponses, allocationIdOf("node-1"), "node-1", shardId);
        fence.validateAndAdvance(term);
        final long seqBeforeLoss = fence.getSeq();

        // The next advance lands but its response is lost. It must be reported as retryable, not as fencing.
        swallowNextResponse.set(true);
        fence.validateAndAdvance(term);
        assertEquals("the landed write should have been adopted", seqBeforeLoss + 1, fence.getSeq());

        // And the chain must still be usable afterwards - a stale token here would surface as a spurious fencing.
        fence.validateAndAdvance(term);
        assertEquals(seqBeforeLoss + 2, fence.getSeq());
    }

    /**
     * A copy that won its path from an equal-term twin, and is then superseded mid-claim, must not delete that path.
     * <p>
     * Withdrawing is the one place a writer would delete at its OWN term - the single exception to "a writer's only
     * destructive act is deleting strictly BELOW its own term", which the rest of the protocol leans on. The object is
     * only ours to remove if we created it: having merely won a CAS against a twin says nothing about whether a further
     * equal-term claimant has taken it over since, in which case deleting would fence a peer that legitimately owns it.
     * {@code FenceTakeover.tla} draws exactly this distinction - {@code VerifyClaim} withdraws only {@code IF wCreated}.
     */
    public void testMidClaimSupersessionDoesNotWithdrawAPathWonFromAnEqualTermTwin() throws IOException {
        final long term = 5;
        final long higherTerm = term + 1;

        // A twin creates the path first, so our claim is forced down the equal-term arbitration branch, not create.
        newFence("twin").validateAndAdvance(term);
        assertTrue(blobContainer.blobExists(RemoteStoreFence.fenceBlobName(term)));

        // Plant a higher term the moment our arbitration CAS lands, so the re-list inside claim() sees a mid-claim
        // supersession. Content does not matter: the listing compares names only.
        final FsBlobContainer planting = new FsBlobContainer(blobStore, BlobPath.cleanPath(), repoPath) {
            private boolean planted;

            @Override
            public String writeBlobConditionally(String blobName, java.io.InputStream in, long size, String expectedToken)
                throws IOException {
                final String token = super.writeBlobConditionally(blobName, in, size, expectedToken);
                if (planted == false && expectedToken != null) {
                    planted = true;
                    final byte[] bytes = new byte[] { 1 };
                    writeBlob(RemoteStoreFence.fenceBlobName(higherTerm), new java.io.ByteArrayInputStream(bytes), bytes.length, true);
                }
                return token;
            }
        };

        final RemoteStoreFence superseded = new RemoteStoreFence(planting, allocationIdOf("node-late"), "node-late", shardId);
        expectThrows(TranslogFencedException.class, () -> superseded.validateAndAdvance(term));

        assertTrue(
            "a path won from an equal-term twin must survive our withdrawal - it may already belong to another peer",
            blobContainer.blobExists(RemoteStoreFence.fenceBlobName(term))
        );
    }

    public void testBootstrapCreatesFence() throws IOException {
        RemoteStoreFence fence = newFence("node-1");
        fence.validateAndAdvance(1);
        assertEquals(1, fence.getTerm());
        assertEquals(0, fence.getSeq());
        assertTrue(blobContainer.blobExists(RemoteStoreFence.fenceBlobName(1)));
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

        // Invariant (fenced is terminal): the same-term takeover rule cannot be used by the zombie to seize the
        // chain back, because a fenced instance never re-enters bootstrap - every retry fails, at any term. Only a
        // new incarnation (a fresh instance, i.e. a cluster-sanctioned transition) may claim the chain again.
        for (int attempt = 0; attempt < randomIntBetween(2, 4); attempt++) {
            expectThrows(TranslogFencedException.class, () -> source.validateAndAdvance(1));
        }
        expectThrows(TranslogFencedException.class, () -> source.validateAndAdvance(2));

        // Target keeps advancing normally
        target.validateAndAdvance(1);
    }

    public void testLocalTermLowerThanFenceTermIsFenced() throws IOException {
        RemoteStoreFence fence = newFence("node-1");
        fence.validateAndAdvance(5);
        RemoteStoreFence stale = newFence("node-2");
        TranslogFencedException e = expectThrows(TranslogFencedException.class, () -> stale.validateAndAdvance(3));
        assertTrue(e.getMessage(), e.getMessage().contains("term [3] superseded by term [5]"));
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
        // Each term's chain starts at seq 0, so one advance after claiming leaves seq 1.
        assertEquals(1, newPrimary.getSeq());
    }

    /**
     * Determinism: a higher-term claimant wins while the incumbent is actively advancing its own chain. Under
     * term-scoped paths the claimant's create-if-absent targets a key the incumbent never writes, so there is no race
     * to lose - no matter how many times the incumbent acknowledges first.
     */
    public void testHigherTermClaimWinsAgainstAnActiveIncumbent() throws IOException {
        RemoteStoreFence incumbent = newFence("node-incumbent");
        incumbent.validateAndAdvance(1);
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            incumbent.validateAndAdvance(1);
        }

        RemoteStoreFence claimant = newFence("node-new");
        claimant.validateAndAdvance(2);
        assertEquals(2, claimant.getTerm());

        // The incumbent's path was deleted outright, so its next acknowledgement fails - and stays failed.
        expectThrows(TranslogFencedException.class, () -> incumbent.validateAndAdvance(1));
        expectThrows(TranslogFencedException.class, () -> incumbent.validateAndAdvance(1));
        assertFalse(blobContainer.blobExists(RemoteStoreFence.fenceBlobName(1)));
        assertTrue(blobContainer.blobExists(RemoteStoreFence.fenceBlobName(2)));
    }

    /**
     * A stale grant is self-limiting. A copy granted term 2 that stalls before ever acknowledging cannot displace the
     * term-3 owner that overtook it: its own path has been deleted, so it fails rather than regressing the fence.
     */
    public void testStaleGrantCannotDisplaceItsSuccessor() throws IOException {
        RemoteStoreFence incumbent = newFence("node-1");
        incumbent.validateAndAdvance(1);

        RemoteStoreFence stalled = newFence("node-2");
        stalled.validateAndAdvance(2);          // claims term 2, then stalls without acknowledging

        RemoteStoreFence successor = newFence("node-3");
        successor.validateAndAdvance(3);
        assertFalse("term 2's path must be deleted by its successor", blobContainer.blobExists(RemoteStoreFence.fenceBlobName(2)));

        // The stalled copy wakes up and tries to acknowledge: its path is gone, so it is fenced and the term-3 owner
        // is untouched.
        expectThrows(TranslogFencedException.class, () -> stalled.validateAndAdvance(2));
        successor.validateAndAdvance(3);
        assertEquals(3, successor.getTerm());
    }

    /**
     * Fenced is terminal, including at a term this instance was never granted. Without explicit terminal state a
     * fenced copy could claim a higher term's path, delete its own successor's, and resurrect itself.
     */
    public void testFencedInstanceCannotClaimAnotherTerm() throws IOException {
        RemoteStoreFence source = newFence("node-source");
        source.validateAndAdvance(1);
        newFence("node-target").validateAndAdvance(1);
        expectThrows(TranslogFencedException.class, () -> source.validateAndAdvance(1));

        expectThrows(TranslogFencedException.class, () -> source.validateAndAdvance(2));
        assertFalse(
            "a fenced instance must not create a path for a term it was never granted",
            blobContainer.blobExists(RemoteStoreFence.fenceBlobName(2))
        );
        assertTrue("the legitimate owner's path must be untouched", blobContainer.blobExists(RemoteStoreFence.fenceBlobName(1)));
    }

    /**
     * Deletion is what fences a lower-term incumbent, and a batch delete can report per-key failures in its response
     * body rather than throwing - S3's DeleteObjects does exactly that, and the shared blob-store helper logs those
     * errors and returns success. A claim must therefore verify its sweep rather than trust it: a surviving lower-term
     * path means that copy still holds a valid token and would keep acknowledging, so proceeding would put two terms on
     * the write path at once.
     */
    public void testClaimRefusesWhenTheSweepSilentlyLeavesALowerTermPath() throws IOException {
        FsBlobContainer droppingDeletes = new FsBlobContainer(blobStore, BlobPath.cleanPath(), repoPath) {
            @Override
            public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) {
                // Silently drop the delete, as a partially-failed batch delete does.
            }
        };

        new RemoteStoreFence(droppingDeletes, allocationIdOf("node-old"), "node-old", shardId).validateAndAdvance(1);

        RemoteStoreFence claimant = new RemoteStoreFence(droppingDeletes, allocationIdOf("node-new"), "node-new", shardId);
        IOException e = expectThrows(IOException.class, () -> claimant.validateAndAdvance(2));
        assertFalse("an unswept predecessor is retryable, not a fencing verdict", e instanceof TranslogFencedException);
        assertTrue(e.getMessage(), e.getMessage().contains("superseded term [1]"));

        // Once the sweep actually lands, the same claim succeeds.
        blobContainer.deleteBlobsIgnoringIfNotExists(List.of(RemoteStoreFence.fenceBlobName(1)));
        newFence("node-new").validateAndAdvance(2);
    }

    /**
     * Supersession is a property of the STORE, not of the asking instance. This is the regression that matters: an
     * earlier version of this check asked "do I still hold my own object?", which answers no both for a superseded copy
     * and for a copy whose fence instance is merely BEHIND the object - engine resets during recovery replace the
     * translog, and so the fence, several times in quick succession. That conflation silenced segment uploads on a
     * healthy shard permanently and hung snapshot-restore recovery until the test suite timed out.
     */
    public void testSupersessionReflectsTheStoreNotTheInstance() throws IOException {
        newFence("node-1").validateAndAdvance(3);

        // An instance that never claimed anything still reports the truth about the store.
        assertFalse(newFence("node-2").isSuperseded(3));
        assertTrue(newFence("node-2").isSuperseded(2));

        // And so does an instance that has itself been fenced.
        RemoteStoreFence fencedInstance = newFence("node-3");
        expectThrows(TranslogFencedException.class, () -> fencedInstance.validateAndAdvance(2));
        assertFalse("a fenced instance must not report supersession at a term nothing has superseded", fencedInstance.isSuperseded(3));
        assertTrue(fencedInstance.isSuperseded(2));
    }

    /**
     * A missing acknowledgement path reports "not superseded", so the segment publish path fails OPEN. Permitting one
     * more publish at our own term leaves an orphan, which is the pre-existing harmless case; failing closed would
     * silence a healthy shard whenever the repository hiccups. The garbage collection paths make the opposite choice.
     */
    public void testMissingPathIsNotTreatedAsSupersession() throws IOException {
        RemoteStoreFence fence = newFence("node-1");
        fence.validateAndAdvance(3);
        assertFalse(fence.isSuperseded(3));

        blobContainer.deleteBlobsIgnoringIfNotExists(List.of(RemoteStoreFence.fenceBlobName(3)));
        assertFalse("an absent path is not evidence of a higher term", fence.isSuperseded(3));
    }

    private RemoteStoreFence newRelocationTarget(String ownerNodeId) {
        return new RemoteStoreFence(blobContainer, allocationIdOf(ownerNodeId), ownerNodeId, shardId, true);
    }

    /**
     * The relocation handoff. Source and target share a term, so they share an object and the term cannot arbitrate
     * between them: the source's recorded handover is what authorizes the target.
     */
    public void testRelocationHandoffTransfersOwnership() throws IOException {
        RemoteStoreFence source = newFence("node-source");
        source.validateAndAdvance(1);

        // Before the handover the target is not the recorded owner and must not take the chain, or it would fence a
        // source that is still serving.
        RemoteStoreFence target = newRelocationTarget("node-target");
        expectThrows(TranslogFencedException.class, () -> target.validateAndAdvance(1));

        source.transferOwnershipTo(1, allocationIdOf("node-target"));
        RemoteStoreFence adopted = newRelocationTarget("node-target");
        adopted.validateAndAdvance(1);
        assertEquals(1, adopted.getTerm());

        // The source's token is now stale, which is correct: the handoff completed.
        expectThrows(TranslogFencedException.class, () -> source.validateAndAdvance(1));
    }

    /**
     * An aborted handoff must not fence a healthy source: the cluster keeps it as primary and releases its upload
     * drains, so it has to be able to resume. The revert succeeds exactly when the target never wrote.
     */
    public void testAbortedHandoffReclaimsOwnershipAndTheSourceResumes() throws IOException {
        RemoteStoreFence source = newFence("node-source");
        source.validateAndAdvance(1);
        source.transferOwnershipTo(1, allocationIdOf("node-target"));

        // The target never took the chain up, so ownership is reclaimable and the source keeps serving.
        assertTrue(source.revertOwnership(1));
        source.validateAndAdvance(1);

        // And a target that activates afterwards finds the source recorded as owner, so it stands down rather than
        // fencing it.
        RemoteStoreFence lateTarget = newRelocationTarget("node-target");
        expectThrows(TranslogFencedException.class, () -> lateTarget.validateAndAdvance(1));
    }

    /** If the target did take the chain up, the handoff effectively completed: the revert must fail and the source stand down. */
    public void testAbortedHandoffAfterTheTargetTookOverStandsDown() throws IOException {
        RemoteStoreFence source = newFence("node-source");
        source.validateAndAdvance(1);
        source.transferOwnershipTo(1, allocationIdOf("node-target"));

        RemoteStoreFence target = newRelocationTarget("node-target");
        target.validateAndAdvance(1);

        assertFalse("the target had taken over, so ownership must not be reclaimable", source.revertOwnership(1));
        expectThrows(TranslogFencedException.class, () -> source.validateAndAdvance(1));
        // The target is unaffected and keeps acknowledging.
        target.validateAndAdvance(1);
    }

    /** A copy that does not own the chain cannot hand it off. */
    public void testTransferRequiresOwnership() throws IOException {
        RemoteStoreFence source = newFence("node-source");
        source.validateAndAdvance(1);
        RemoteStoreFence stranger = newFence("node-other");
        expectThrows(TranslogFencedException.class, () -> stranger.transferOwnershipTo(1, allocationIdOf("node-target")));
    }

    /**
     * An ownership transfer whose response is lost must not leave the source holding a stale token. The write landed -
     * the object records the target - so the source adopts the resulting token by identity, exactly as the ack path
     * does. What makes this matter is the abort path: with a stale token, an aborted handoff's revert would lose its
     * CAS and wrongly conclude the target took over, standing down a healthy source.
     */
    public void testLostResponseOnTheTransferDoesNotStrandTheSource() throws IOException {
        final AtomicBoolean swallowNextResponse = new AtomicBoolean();
        final FsBlobContainer losingResponses = new FsBlobContainer(blobStore, BlobPath.cleanPath(), repoPath) {
            @Override
            public String writeBlobConditionally(String blobName, java.io.InputStream in, long size, String expectedToken)
                throws IOException {
                final String token = super.writeBlobConditionally(blobName, in, size, expectedToken);
                if (swallowNextResponse.compareAndSet(true, false)) {
                    throw new IOException("connection reset before the response was read");
                }
                return token;
            }
        };
        final RemoteStoreFence source = new RemoteStoreFence(losingResponses, allocationIdOf("node-source"), "node-source", shardId);
        source.validateAndAdvance(1);

        // The transfer lands but its response is lost: it must be reported as complete, with the token adopted.
        swallowNextResponse.set(true);
        source.transferOwnershipTo(1, allocationIdOf("node-target"));

        // The abort path can now genuinely distinguish the outcomes: the target never wrote, so the revert succeeds
        // and the source resumes - instead of losing a CAS against its own transfer and standing down for no reason.
        assertTrue("the revert must succeed against the adopted token", source.revertOwnership(1));
        source.validateAndAdvance(1);
    }

    /** The revert twin: a revert that lands with a lost response reports reclaimed, with the token adopted. */
    public void testLostResponseOnTheRevertDoesNotStandTheSourceDown() throws IOException {
        final AtomicBoolean swallowNextResponse = new AtomicBoolean();
        final FsBlobContainer losingResponses = new FsBlobContainer(blobStore, BlobPath.cleanPath(), repoPath) {
            @Override
            public String writeBlobConditionally(String blobName, java.io.InputStream in, long size, String expectedToken)
                throws IOException {
                final String token = super.writeBlobConditionally(blobName, in, size, expectedToken);
                if (swallowNextResponse.compareAndSet(true, false)) {
                    throw new IOException("connection reset before the response was read");
                }
                return token;
            }
        };
        final RemoteStoreFence source = new RemoteStoreFence(losingResponses, allocationIdOf("node-source"), "node-source", shardId);
        source.validateAndAdvance(1);
        source.transferOwnershipTo(1, allocationIdOf("node-target"));

        swallowNextResponse.set(true);
        assertTrue("a revert that landed must be reported as reclaimed", source.revertOwnership(1));
        // And the source serves on, holding the current token.
        source.validateAndAdvance(1);
    }

    /** A fence instance requiring recorded ownership, as every translog instance does - see RemoteFsTranslog#buildFence. */
    private RemoteStoreFence newTranslogInstance(String ownerNodeId) {
        return new RemoteStoreFence(blobContainer, allocationIdOf(ownerNodeId), ownerNodeId, shardId, true);
    }

    /**
     * The re-adoption regression, straight from the {@code FenceTakeover.tla} counterexample. The fence is claimed
     * twice: the recovery seal claims via a throwaway instance whose token is discarded, the restore point is read
     * with no live token held, and the translog instance claims again - re-adopts - on its first upload. If an
     * equal-term twin legitimately
     * claimed the chain during that window, the re-adoption must be REFUSED: taking the chain back would serve from a
     * restore point read before the twin's acknowledgements, and those acknowledged writes would be lost.
     */
    public void testTranslogInstanceMustNotReAdoptAChainAnEqualTermTwinClaimed() throws IOException {
        // First claim: the seal instance claims at term 1, recording this copy's allocation id, and is discarded.
        newFence("node-1").validateAndAdvance(1);

        // The window: an equal-term twin arbitrates the chain over and acknowledges a write through it.
        RemoteStoreFence twin = newFence("node-twin");
        twin.validateAndAdvance(1);
        twin.validateAndAdvance(1);

        // Second claim: this copy's translog instance is not the recorded owner any more, so it must be fenced - never
        // steal the chain back.
        RemoteStoreFence translogInstance = newTranslogInstance("node-1");
        TranslogFencedException e = expectThrows(TranslogFencedException.class, () -> translogInstance.validateAndAdvance(1));
        assertTrue(e.getMessage(), e.getMessage().contains("not the recorded owner"));

        // The twin - the legitimate owner whose acknowledged write would have been lost - is untouched.
        twin.validateAndAdvance(1);
    }

    /** The normal flow: the seal recorded this copy as owner, so its translog instance re-adopts the chain. */
    public void testTranslogInstanceAdoptsTheChainItsOwnSealRecorded() throws IOException {
        newFence("node-1").validateAndAdvance(1); // first claim: the seal records this copy's allocation id
        RemoteStoreFence translogInstance = newTranslogInstance("node-1");
        translogInstance.validateAndAdvance(1); // second claim: re-adopts the chain
        translogInstance.validateAndAdvance(1); // and acknowledges through it
        assertEquals(1, translogInstance.getTerm());
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
            blobContainer.readBlobWithVersion(RemoteStoreFence.fenceBlobName(1)).content()
        );
        assertEquals(1, finalState.term);
        assertTrue(finalState.nodeId.startsWith("node-"));
        assertEquals(allocationIdOf(finalState.nodeId), finalState.allocationId);
    }

    public void testFenceBlobIsSelfDescribing() throws IOException {
        RemoteStoreFence fence = newFence("node-1");
        fence.validateAndAdvance(9);
        RemoteStoreFence.FenceState state = RemoteStoreFence.FenceState.parse(
            blobContainer.readBlobWithVersion(RemoteStoreFence.fenceBlobName(9)).content()
        );
        assertEquals(9, state.term);
        assertEquals(allocationIdOf("node-1"), state.allocationId);
        assertEquals("node-1", state.nodeId);
        assertEquals(0, state.seq);
        // The blob records the shard identity it belongs to (one blob per shard)
        assertEquals(shardId.getIndex().getUUID(), state.indexUUID);
        assertEquals(shardId.id(), state.shardId);
    }

    public void testFenceStateCodecRoundTrip() throws IOException {
        RemoteStoreFence.FenceState state = new RemoteStoreFence.FenceState("uuid", 3, 7, "alloc-a", "node-a", 42);
        RemoteStoreFence.FenceState parsed = RemoteStoreFence.FenceState.parse(state.toBytes());
        assertEquals("uuid", parsed.indexUUID);
        assertEquals(3, parsed.shardId);
        assertEquals(7, parsed.term);
        assertEquals("alloc-a", parsed.allocationId);
        assertEquals("node-a", parsed.nodeId);
        assertEquals(42, parsed.seq);
    }

    public void testFenceStateCodecRejectsGarbage() {
        expectThrows(IOException.class, () -> RemoteStoreFence.FenceState.parse("garbage".getBytes(StandardCharsets.UTF_8)));
        // Wrong codec version
        expectThrows(
            IOException.class,
            () -> RemoteStoreFence.FenceState.parse("v2|uuid|0|1|alloc-1|node-1|0".getBytes(StandardCharsets.UTF_8))
        );
        // Non-numeric term
        expectThrows(
            IOException.class,
            () -> RemoteStoreFence.FenceState.parse("v1|uuid|0|x|alloc-1|node-1|0".getBytes(StandardCharsets.UTF_8))
        );
        // Non-numeric shard id
        expectThrows(
            IOException.class,
            () -> RemoteStoreFence.FenceState.parse("v1|uuid|x|1|alloc-1|node-1|0".getBytes(StandardCharsets.UTF_8))
        );
        // Truncated
        expectThrows(
            IOException.class,
            () -> RemoteStoreFence.FenceState.parse("v1|uuid|0|1|alloc-1|node-1".getBytes(StandardCharsets.UTF_8))
        );
    }

    /**
     * The separator is unescaped, so a string field containing it would encode a blob that {@code parse} rejects -
     * which would fence a healthy primary. Index UUIDs, allocation ids and node ids are base64 UUIDs so this cannot
     * happen today; reject it at the source rather than rely on that.
     */
    public void testFenceStateRejectsFieldsContainingTheSeparator() {
        expectThrows(IllegalArgumentException.class, () -> new RemoteStoreFence.FenceState("uuid", 0, 1, "alloc-1", "node|1", 0));
        expectThrows(IllegalArgumentException.class, () -> new RemoteStoreFence.FenceState("uuid", 0, 1, "alloc|1", "node-1", 0));
        expectThrows(IllegalArgumentException.class, () -> new RemoteStoreFence.FenceState("uu|id", 0, 1, "alloc-1", "node-1", 0));
        // and the encoding of well-formed fields still round-trips
        RemoteStoreFence.FenceState roundTripped = expectSuccess(new RemoteStoreFence.FenceState("uuid", 0, 7, "alloc-1", "node-1", 3));
        assertEquals(7, roundTripped.term);
        assertEquals("alloc-1", roundTripped.allocationId);
        assertEquals("node-1", roundTripped.nodeId);
        assertEquals(3, roundTripped.seq);
    }

    /**
     * Invariant (one blob per shard): a fence blob describing a different shard at this key means two shards resolved the same path.
     * That must surface as a loud repository error - never as a fencing verdict on a chain that is not ours.
     */
    public void testFenceBlobForAnotherShardFailsLoudly() throws IOException {
        byte[] foreign = new RemoteStoreFence.FenceState("other-uuid", 3, 1, "alloc-x", "node-x", 0).toBytes();
        blobContainer.writeBlobConditionally(RemoteStoreFence.fenceBlobName(5), new ByteArrayInputStream(foreign), foreign.length, null);
        RemoteStoreFence fence = newFence("node-1");
        IOException e = expectThrows(IOException.class, () -> fence.validateAndAdvance(5));
        assertFalse("a shard identity mismatch must not be classified as fenced", e instanceof TranslogFencedException);
        assertTrue(e.getMessage(), e.getMessage().contains("different shard"));
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
        RemoteStoreFence fence = new RemoteStoreFence(unsupported, allocationIdOf("node-1"), "node-1", shardId);
        expectThrows(UnsupportedOperationException.class, () -> fence.validateAndAdvance(1));
    }
}
