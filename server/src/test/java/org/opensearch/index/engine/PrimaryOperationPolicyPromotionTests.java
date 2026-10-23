/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.Term;
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingHelper;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.exec.EngineBackedIndexerFactory;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.index.shard.IndexShardTestUtils;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.indices.recovery.RecoveryTarget;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link PrimaryOperationPolicy} is only consulted for primary-origin operations, so a shard that has
 * been a replica for a while never exercises the policy its engine resolved at construction. These tests
 * cover the document replication promotion path, which unlike segment replication does not rebuild the
 * engine, and would therefore keep serving that construction-time policy as a primary.
 */
public class PrimaryOperationPolicyPromotionTests extends IndexShardTestCase {

    /**
     * Stands in for a plugin whose {@code getPrimaryOperationPolicy} answer follows an updatable index
     * setting: the policy behind the reference can be changed after the engine has been built.
     */
    private IndexShard newReplicaWithPluginPolicy(AtomicReference<PrimaryOperationPolicy> pluginPolicy) throws IOException {
        return newStartedShardWithPluginPolicy(pluginPolicy, false);
    }

    private IndexShard newStartedShardWithPluginPolicy(AtomicReference<PrimaryOperationPolicy> pluginPolicy, boolean primary)
        throws IOException {
        return newStartedShard(
            p -> newShard(
                p,
                Settings.EMPTY,
                new EngineBackedIndexerFactory(
                    config -> new InternalEngine(config.toBuilder().primaryOperationPolicySupplier(pluginPolicy::get).build())
                )
            ),
            primary
        );
    }

    /** Leaves a gap at sequence number 0 by applying operations 1 and 2 only. */
    private void indexOnReplicaLeavingGapAtZero(IndexShard replica) throws IOException {
        for (long seqNo : new long[] { 1L, 2L }) {
            replica.applyIndexOperationOnReplica(
                UUID.randomUUID().toString(),
                seqNo,
                replica.getOperationPrimaryTerm(),
                1,
                IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
                false,
                new SourceToParse(replica.shardId().getIndexName(), Long.toString(seqNo), new BytesArray("{}"), MediaTypeRegistry.JSON)
            );
        }
        assertEquals(SequenceNumbers.NO_OPS_PERFORMED, replica.getLocalCheckpoint());
        assertEquals(2L, replica.seqNoStats().getMaxSeqNo());
    }

    /**
     * The promotion path fills sequence-number gaps, so whether the gap at 0 survives promotion tells us
     * which policy the engine used. A policy installed after the engine was built must be the one that
     * applies, otherwise this shard would record no-ops that can collide with sequence numbers the
     * upstream authority has not replicated to it yet.
     */
    public void testPromotionPicksUpPolicyInstalledAfterEngineWasBuilt() throws Exception {
        final AtomicReference<PrimaryOperationPolicy> pluginPolicy = new AtomicReference<>(DefaultPrimaryOperationPolicy.INSTANCE);
        final IndexShard replica = newReplicaWithPluginPolicy(pluginPolicy);
        try {
            indexOnReplicaLeavingGapAtZero(replica);

            // the plugin starts overriding the policy while this shard is still a replica, so nothing
            // rebuilds its engine
            pluginPolicy.set(FakePreAssignedSeqNoPrimaryOperationPolicy.INSTANCE);
            assertSame(
                "a replica never refreshes, so its engine must still hold the construction-time policy",
                DefaultPrimaryOperationPolicy.INSTANCE,
                replica.getPrimaryOperationPolicy()
            );

            promote(replica);
            assertSame(
                "promotion must install the policy the plugin resolves now",
                FakePreAssignedSeqNoPrimaryOperationPolicy.INSTANCE,
                replica.getPrimaryOperationPolicy()
            );
            assertEquals(
                "the gap must not be filled under a policy whose seq no. space is owned upstream",
                SequenceNumbers.NO_OPS_PERFORMED,
                replica.getLocalCheckpoint()
            );
        } finally {
            closeShards(replica);
        }
    }

    /** The unchanged case: with no plugin override the promoted primary fills its gaps as it always has. */
    public void testPromotionUnderDefaultPolicyStillFillsGaps() throws Exception {
        final AtomicReference<PrimaryOperationPolicy> pluginPolicy = new AtomicReference<>(DefaultPrimaryOperationPolicy.INSTANCE);
        final IndexShard replica = newReplicaWithPluginPolicy(pluginPolicy);
        try {
            indexOnReplicaLeavingGapAtZero(replica);

            promote(replica);
            assertSame(DefaultPrimaryOperationPolicy.INSTANCE, replica.getPrimaryOperationPolicy());
            assertEquals(2L, replica.getLocalCheckpoint());
        } finally {
            closeShards(replica);
        }
    }

    /**
     * A primary relocation target builds its engine at the start of peer recovery and becomes a primary at
     * handoff, through {@link IndexShard#activateWithPrimaryContext}, without a primary term bump or an
     * engine reset. A policy the plugin starts resolving while the recovery is still running must be the
     * one the relocated primary uses. Segment replication is not covered here because its relocation
     * handoff rebuilds the engine via {@link IndexShard#resetToWriteableEngine()}.
     */
    public void testRelocationHandoffPicksUpPolicyInstalledAfterEngineWasBuilt() throws Exception {
        final IndexShard source = newStartedShard(true);
        IndexShard target = null;
        try {
            final int docs = randomIntBetween(0, 10);
            for (int i = 0; i < docs; i++) {
                indexDoc(source, "_doc", Integer.toString(i));
            }
            IndexShardTestCase.updateRoutingEntry(source, source.routingEntry().relocate(randomAlphaOfLength(10), -1));

            final AtomicReference<PrimaryOperationPolicy> pluginPolicy = new AtomicReference<>(DefaultPrimaryOperationPolicy.INSTANCE);
            final AtomicReference<InternalEngine> targetEngine = new AtomicReference<>();
            target = newShard(source.routingEntry().getTargetRelocatingShard(), Settings.EMPTY, new EngineBackedIndexerFactory(config -> {
                final InternalEngine engine = new InternalEngine(
                    config.toBuilder().primaryOperationPolicySupplier(pluginPolicy::get).build()
                );
                targetEngine.set(engine);
                return engine;
            }));
            updateMappings(target, source.indexSettings().getIndexMetadata());

            recoverReplica(target, source, (shard, sourceNode) -> new RecoveryTarget(shard, sourceNode, recoveryListener, threadPool) {
                @Override
                public void finalizeRecovery(long globalCheckpoint, long trimAboveSeqNo, ActionListener<Void> listener) {
                    // the target's engine is open by now, so the switch below happens after it resolved its policy
                    assertNotNull("the target engine must be open before the policy changes", targetEngine.get());
                    // the plugin starts overriding the policy while the target is still recovering; nothing
                    // rebuilds the target's engine between here and the handoff
                    pluginPolicy.set(FakePreAssignedSeqNoPrimaryOperationPolicy.INSTANCE);
                    assertSame(
                        "the recovering target must still hold the construction-time policy before the handoff",
                        DefaultPrimaryOperationPolicy.INSTANCE,
                        targetEngine.get().getPrimaryOperationPolicy()
                    );
                    super.finalizeRecovery(globalCheckpoint, trimAboveSeqNo, listener);
                }
            }, true, true);
            assertTrue("the target must have been handed primary mode", target.isPrimaryMode());
            assertTrue(source.isRelocatedPrimary());

            assertUpstreamSeqNoAppliedVerbatim(target, targetEngine.get(), docs + 100L);
        } finally {
            closeShards(source, target);
        }
    }

    /**
     * A primary recovering from its store builds its engine at the start of recovery and enters primary
     * mode only when the cluster manager marks it started, through the same-term branch of
     * {@link IndexShard#updateShardState}. Neither a primary term bump nor an engine reset happens in
     * between, and translog replay or a large restore can keep the shard recovering for a long time, so a
     * policy the plugin starts resolving during that window must be the one the started primary uses.
     */
    public void testStartingRecoveredPrimaryPicksUpPolicyInstalledAfterEngineWasBuilt() throws Exception {
        final AtomicReference<PrimaryOperationPolicy> pluginPolicy = new AtomicReference<>(DefaultPrimaryOperationPolicy.INSTANCE);
        final AtomicReference<InternalEngine> primaryEngine = new AtomicReference<>();
        final IndexShard primary = newShard(true, Settings.EMPTY, new EngineBackedIndexerFactory(config -> {
            final InternalEngine engine = new InternalEngine(config.toBuilder().primaryOperationPolicySupplier(pluginPolicy::get).build());
            primaryEngine.set(engine);
            return engine;
        }));
        try {
            primary.markAsRecovering(
                "store",
                new RecoveryState(
                    primary.routingEntry(),
                    IndexShardTestUtils.getFakeDiscoNode(primary.routingEntry().currentNodeId()),
                    null
                )
            );
            assertTrue(recoverFromStore(primary));
            assertNotNull("the engine must be open before the policy changes", primaryEngine.get());
            assertFalse("the shard must not be in primary mode until it is started", primary.isPrimaryMode());

            // the plugin starts overriding the policy while the primary is still recovering; nothing rebuilds
            // its engine between here and the shard being started
            pluginPolicy.set(FakePreAssignedSeqNoPrimaryOperationPolicy.INSTANCE);
            assertSame(
                "a recovering primary must still hold the construction-time policy until it is started",
                DefaultPrimaryOperationPolicy.INSTANCE,
                primary.getPrimaryOperationPolicy()
            );

            updateRoutingEntry(primary, ShardRoutingHelper.moveToStarted(primary.routingEntry()));
            assertTrue("starting the recovered primary must activate primary mode", primary.isPrimaryMode());

            assertUpstreamSeqNoAppliedVerbatim(primary, primaryEngine.get(), 100L);
        } finally {
            closeShards(primary);
        }
    }

    /**
     * A plugin whose policy is keyed off an updatable setting, and that has nothing else to rebuild when
     * that setting changes, can refresh a started primary in place rather than resetting its engine. The
     * shard-level accessor must report the engine's snapshot, not the config's live resolution, so the
     * plugin can tell whether the engine has picked the change up yet.
     */
    public void testStartedPrimaryCanBeRefreshedInPlace() throws Exception {
        final AtomicReference<PrimaryOperationPolicy> pluginPolicy = new AtomicReference<>(DefaultPrimaryOperationPolicy.INSTANCE);
        final IndexShard primary = newStartedShardWithPluginPolicy(pluginPolicy, true);
        try {
            assertSame(DefaultPrimaryOperationPolicy.INSTANCE, primary.getPrimaryOperationPolicy());

            pluginPolicy.set(FakePreAssignedSeqNoPrimaryOperationPolicy.INSTANCE);
            assertSame(
                "the shard must report the engine's snapshot, not the config's live resolution",
                DefaultPrimaryOperationPolicy.INSTANCE,
                primary.getPrimaryOperationPolicy()
            );

            primary.refreshPrimaryOperationPolicy();
            assertSame(FakePreAssignedSeqNoPrimaryOperationPolicy.INSTANCE, primary.getPrimaryOperationPolicy());
            assertEquals("the refresh must release its operation block", 0, primary.getActiveOperationsCount());

            pluginPolicy.set(DefaultPrimaryOperationPolicy.INSTANCE);
            primary.refreshPrimaryOperationPolicy();
            assertSame(
                "a refresh must be able to switch back",
                DefaultPrimaryOperationPolicy.INSTANCE,
                primary.getPrimaryOperationPolicy()
            );
        } finally {
            closeShards(primary);
        }
    }

    public void testPolicyAccessorOnClosedShardThrows() throws Exception {
        final IndexShard shard = newStartedShard(true);
        closeShards(shard);
        expectThrows(AlreadyClosedException.class, shard::getPrimaryOperationPolicy);
    }

    /**
     * Asserts the shard is serving primary operations under the pre-assigned policy, first directly and
     * then behaviorally: applies a primary-origin operation carrying an upstream-assigned sequence number
     * directly to the shard's engine. Such an operation is only valid under the pre-assigned policy; the
     * default policy the engine resolved when it was built rejects it, so whether it succeeds confirms
     * which policy the shard is actually applying.
     */
    private void assertUpstreamSeqNoAppliedVerbatim(IndexShard shard, InternalEngine engine, long upstreamSeqNo) throws IOException {
        assertSame(
            "entering primary mode must install the policy the plugin resolves now",
            FakePreAssignedSeqNoPrimaryOperationPolicy.INSTANCE,
            shard.getPrimaryOperationPolicy()
        );
        final ParsedDocument doc = shard.mapperService()
            .documentMapper()
            .parse(new SourceToParse(shard.shardId().getIndexName(), "upstream", new BytesArray("{}"), MediaTypeRegistry.JSON));
        final Engine.Index upstreamOp = new Engine.Index(
            new Term(IdFieldMapper.NAME, Uid.encodeId(doc.id())),
            doc,
            upstreamSeqNo,
            shard.getOperationPrimaryTerm(),
            1L,
            VersionType.EXTERNAL,
            Engine.Operation.Origin.PRIMARY,
            System.nanoTime(),
            IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
            false,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            SequenceNumbers.UNASSIGNED_PRIMARY_TERM
        );
        final Engine.IndexResult result = engine.index(upstreamOp);
        assertEquals(Engine.Result.Type.SUCCESS, result.getResultType());
        assertEquals("the upstream seq no. must be applied verbatim by the primary", upstreamSeqNo, result.getSeqNo());
    }

    /**
     * Promotes the shard and waits for the primary term bump to complete, which is what runs the gap
     * filling this test observes.
     */
    private void promote(IndexShard replica) throws Exception {
        final ShardRouting replicaRouting = replica.routingEntry();
        promoteReplica(
            replica,
            Collections.singleton(replicaRouting.allocationId().getId()),
            new IndexShardRoutingTable.Builder(replicaRouting.shardId()).addShard(replicaRouting).build()
        );
        final PlainActionFuture<Releasable> permit = new PlainActionFuture<>();
        replica.acquirePrimaryOperationPermit(permit, ThreadPool.Names.GENERIC, "");
        permit.get().close();
    }
}
