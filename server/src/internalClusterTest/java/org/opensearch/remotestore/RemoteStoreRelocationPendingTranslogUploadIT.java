/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AbstractTokenFilterFactory;
import org.opensearch.index.analysis.TokenFilterFactory;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.index.translog.InternalTranslogManager;
import org.opensearch.index.translog.RemoteFsTranslog;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.analysis.AnalysisModule;
import org.opensearch.indices.recovery.PeerRecoveryTargetService;
import org.opensearch.plugins.AnalysisPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singletonMap;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Primary relocation of a remote-store index under REQUEST durability must not lose acknowledged operations.
 * <p>
 * Under REQUEST durability a write is acknowledged after {@code RemoteFsTranslog.ensureSynced}; when another upload holds the
 * single sync permit, {@code ensureSynced} returns {@code false} without uploading and the write is still acknowledged, with the
 * operation only in the local writer. The hand-off ({@code IndexShard.relocated}) drains uploads and then hands over to a target
 * that rebuilds from the remote store; anything not uploaded by then is lost. Lost operations below the remote translog's
 * highest sequence number leave a gap that pins the new primary's local checkpoint and, with it, translog trimming
 * ({@code minSeqNoToKeep}) for the life of that primary; lost operations above it simply vanish and their sequence numbers
 * are reused.
 * <p>
 * The interleaving is made deterministic with three controls: a test token filter parks document B inside analysis after its
 * sequence number is assigned; the recovery is held just before FINALIZE so the writes fall after the phase-2 history
 * snapshot (whose refresh uploads segments at the processed checkpoint and would otherwise protect them); and the sync permit is
 * held through {@code drainSync()} while B and D are acknowledged, standing in for an in-flight generation-roll upload.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStoreRelocationPendingTranslogUploadIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "relocation-pending-upload";
    private static final String BLOCK_TOKEN = "blockme";

    private static volatile CountDownLatch blockedDocArrived = new CountDownLatch(1);
    private static volatile CountDownLatch releaseBlockedDoc = new CountDownLatch(1);

    /** Parks the indexing thread when it analyses {@link #BLOCK_TOKEN}; the sequence number is already assigned by then. */
    public static class BlockingAnalysisPlugin extends Plugin implements AnalysisPlugin {
        @Override
        public Map<String, AnalysisModule.AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
            return singletonMap(
                "block_filter",
                (indexSettings, environment, name, settings) -> new AbstractTokenFilterFactory(indexSettings, name, settings) {
                    @Override
                    public TokenStream create(TokenStream tokenStream) {
                        return new TokenFilter(tokenStream) {
                            private final CharTermAttribute term = addAttribute(CharTermAttribute.class);

                            @Override
                            public boolean incrementToken() throws IOException {
                                if (input.incrementToken() == false) {
                                    return false;
                                }
                                if (BLOCK_TOKEN.contentEquals(term)) {
                                    blockedDocArrived.countDown();
                                    try {
                                        releaseBlockedDoc.await(2, TimeUnit.MINUTES);
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                    }
                                }
                                return true;
                            }
                        };
                    }
                }
            );
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(BlockingAnalysisPlugin.class, MockTransportService.TestPlugin.class))
            .collect(Collectors.toList());
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        // The parked document occupies one write thread; the other writes still need one.
        int processors = Math.max(2, Math.min(4, Runtime.getRuntime().availableProcessors()));
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(OpenSearchExecutors.NODE_PROCESSORS_SETTING.getKey(), processors)
            .build();
    }

    public void testAckedOperationsPendingUploadSurviveRelocation() throws Exception {
        blockedDocArrived = new CountDownLatch(1);
        releaseBlockedDoc = new CountDownLatch(1);

        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);

        Settings indexSettings = Settings.builder()
            .put(remoteStoreIndexSettings(0, 1))
            .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "-1") // no background refresh: only explicit uploads
            .put("index.analysis.analyzer.blocking.type", "custom")
            .put("index.analysis.analyzer.blocking.tokenizer", "standard")
            .putList("index.analysis.analyzer.blocking.filter", "block_filter")
            .build();
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("text")
            .field("type", "text")
            .field("analyzer", "blocking")
            .endObject()
            .endObject()
            .endObject();
        assertAcked(prepareCreate(INDEX_NAME).setSettings(indexSettings).setMapping(mapping));
        ensureGreen(INDEX_NAME);

        final String sourceNode = primaryNodeName(INDEX_NAME);
        final String targetNode = internalCluster().getDataNodeNames()
            .stream()
            .filter(n -> n.equals(sourceNode) == false)
            .findFirst()
            .get();
        final IndexShard source = getIndexShard(sourceNode, INDEX_NAME);
        final InternalEngine sourceEngine = (InternalEngine) IndexShardTestCase.getEngine(source);
        final RemoteFsTranslog sourceTranslog = (RemoteFsTranslog) ((InternalTranslogManager) sourceEngine.translogManager()).getTranslog();

        // A: regular write, then refresh so the remote segment metadata carries checkpoint 0.
        final long genA = sourceTranslog.currentFileGeneration();
        IndexResponse a = client().prepareIndex(INDEX_NAME).setId("A").setSource("text", "alpha").get();
        final long seqA = a.getSeqNo();
        assertBusy(() -> assertTrue(sourceTranslog.allUploaded().contains(Translog.getFilename(genA))));
        refresh(INDEX_NAME);

        // Start the relocation and hold it just before FINALIZE, so the writes below fall after the phase-2 refresh.
        final CountDownLatch finalizeReached = new CountDownLatch(1);
        final CountDownLatch releaseFinalize = new CountDownLatch(1);
        final MockTransportService sourceTransport = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            sourceNode
        );
        sourceTransport.addSendBehavior((connection, requestId, action, request, options) -> {
            if (PeerRecoveryTargetService.Actions.FINALIZE.equals(action)) {
                finalizeReached.countDown();
                try {
                    assertTrue(releaseFinalize.await(2, TimeUnit.MINUTES));
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });
        assertAcked(
            client().admin()
                .cluster()
                .prepareReroute()
                .add(new MoveAllocationCommand(INDEX_NAME, 0, sourceNode, targetNode))
                .execute()
                .actionGet()
        );
        assertTrue("recovery never reached FINALIZE", finalizeReached.await(60, TimeUnit.SECONDS));
        assertEquals("phase 1/2 refreshed at checkpoint of A", seqA, sourceEngine.lastRefreshedCheckpoint());

        // B takes seqNo A+1 and parks in analysis; C takes A+2 and is uploaded. Remote translog now ends at A+2 without A+1.
        ActionFuture<IndexResponse> bFuture = client().prepareIndex(INDEX_NAME).setId("B").setSource("text", BLOCK_TOKEN).execute();
        assertTrue("B never reached the blocking token filter", blockedDocArrived.await(30, TimeUnit.SECONDS));
        final long genC = sourceTranslog.currentFileGeneration();
        IndexResponse c = client().prepareIndex(INDEX_NAME).setId("C").setSource("text", "charlie").get();
        final long seqC = c.getSeqNo();
        assertEquals(seqA + 2, seqC);
        assertBusy(() -> assertTrue(sourceTranslog.allUploaded().contains(Translog.getFilename(genC))));
        final long genPending = sourceTranslog.currentFileGeneration();
        assertTrue(genPending > genC);

        // Hold the sync permit (stands in for an in-flight generation-roll upload), then release B and index D.
        // Both are acknowledged while their generation is local-only.
        final Releasable permitHolder = sourceEngine.translogManager().drainSync();
        releaseBlockedDoc.countDown();
        IndexResponse b = bFuture.actionGet(TimeValue.timeValueSeconds(30));
        assertEquals(RestStatus.CREATED, b.status());
        final long seqB = b.getSeqNo();
        assertEquals(seqA + 1, seqB);
        IndexResponse d = client().prepareIndex(INDEX_NAME).setId("D").setSource("text", "delta").get();
        assertEquals(RestStatus.CREATED, d.status());
        final long seqD = d.getSeqNo();
        assertEquals(seqC + 1, seqD);
        assertEquals(seqD, source.getProcessedLocalCheckpoint());
        assertEquals("B and D not persisted: their syncs were skipped", seqA, source.getLocalCheckpoint());
        assertEquals(genPending, sourceTranslog.currentFileGeneration());
        assertFalse("B and D's generation is local-only", sourceTranslog.allUploaded().contains(Translog.getFilename(genPending)));

        // Let the recovery continue. relocated() blocks operations and waits in drainSync() for the permit we hold.
        releaseFinalize.countDown();
        assertBusy(
            () -> assertEquals(
                "relocated() should be waiting with operations blocked",
                IndexShard.OPERATIONS_BLOCKED,
                source.getActiveOperationsCount()
            ),
            60,
            TimeUnit.SECONDS
        );
        // Hand the permit to relocated()'s drainSync(). It must upload the pending generation before the hand-off completes.
        permitHolder.close();

        ensureGreen(TimeValue.timeValueSeconds(60), INDEX_NAME);
        assertBusy(() -> {
            assertEquals(
                ShardRoutingState.STARTED,
                client().admin().cluster().prepareState().get().getState().routingTable().index(INDEX_NAME).shard(0).primaryShard().state()
            );
            assertEquals(targetNode, primaryNodeName(INDEX_NAME));
        }, 60, TimeUnit.SECONDS);
        sourceTransport.clearAllRules();

        // The hand-off uploaded B and D's generation.
        assertTrue(sourceTranslog.allUploaded().contains(Translog.getFilename(genPending)));

        // New primary: complete history, both acknowledged documents present, no reused sequence number.
        final IndexShard target = getIndexShard(targetNode, INDEX_NAME);
        final RemoteFsTranslog targetTranslog = (RemoteFsTranslog) ((InternalTranslogManager) ((InternalEngine) IndexShardTestCase
            .getEngine(target)).translogManager()).getTranslog();
        assertEquals(seqD, target.seqNoStats().getMaxSeqNo());
        assertEquals(seqD, target.getProcessedLocalCheckpoint());
        assertEquals(seqD, target.getLocalCheckpoint());
        assertTrue(client().prepareGet(INDEX_NAME, "A").get().isExists());
        assertTrue("acknowledged write B survived the relocation", client().prepareGet(INDEX_NAME, "B").get().isExists());
        assertTrue(client().prepareGet(INDEX_NAME, "C").get().isExists());
        assertTrue("acknowledged write D survived the relocation", client().prepareGet(INDEX_NAME, "D").get().isExists());
        IndexResponse e = client().prepareIndex(INDEX_NAME).setId("E").setSource("text", "echo").get();
        assertEquals("no sequence number is reused", seqD + 1, e.getSeqNo());

        // Translog trimming keeps advancing on the new primary.
        final long minGenAfterHandoff = targetTranslog.getMinFileGeneration();
        final int extraDocs = 20;
        for (int i = 0; i < extraDocs; i++) {
            client().prepareIndex(INDEX_NAME).setId("extra-" + i).setSource("text", "extra").get();
        }
        refresh(INDEX_NAME);
        flush(INDEX_NAME);
        refresh(INDEX_NAME);
        assertEquals(seqD + 1 + extraDocs, target.seqNoStats().getMaxSeqNo());
        assertEquals(target.seqNoStats().getMaxSeqNo(), target.getProcessedLocalCheckpoint());
        assertBusy(() -> assertTrue("translog floor advances after refresh", targetTranslog.getMinFileGeneration() > minGenAfterHandoff));
    }
}
