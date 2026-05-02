/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.dataformat.stub.FileBackedDataFormatPlugin;
import org.opensearch.index.engine.dataformat.stub.InMemoryCommitter;
import org.opensearch.index.engine.exec.commit.CommitterFactory;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * IT for committer failure injection. Uses {@link FailureInjectingCommitterPlugin} which provides
 * an {@link InMemoryCommitter} with configurable commit failures.
 * <p>
 * Tests:
 * <ul>
 *   <li>{@code testCommitCorruptionFailsEngineAndMarksStore} — CorruptIndexException during commit</li>
 *   <li>{@code testCommitIOExceptionEngineStaysOpen} — plain IOException during commit, engine recovers</li>
 *   <li>{@code testCommitFailureThenRecovery} — commit fails, clear, flush succeeds</li>
 * </ul>
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class CompositeCommitterFailureIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "test-committer-failure";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(FileBackedDataFormatPlugin.class, CompositeDataFormatPlugin.class, FailureInjectingCommitterPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .build();
    }

    @Override
    public void tearDown() throws Exception {
        FailureInjectingCommitterPlugin.clearFailure();
        FileBackedDataFormatPlugin.clearFailure();
        super.tearDown();
    }

    /** CorruptIndexException during commit → engine fails. */
    public void testCommitCorruptionFailsEngine() throws Exception {
        createCompositeIndex();
        indexDocs(5);

        FailureInjectingCommitterPlugin.setCommitFailure(
            () -> new org.apache.lucene.index.CorruptIndexException("simulated corruption", "test")
        );

        try {
            flush();
        } catch (Exception e) {
            // expected — FlushFailedEngineException
        }

        FailureInjectingCommitterPlugin.clearFailure();

        // Engine should be failed — subsequent ops throw
        try {
            indexDocs(1);
            fail("expected exception after engine failure");
        } catch (Exception e) {
            // expected
        }
    }

    /** Plain IOException during commit → engine stays open, retry succeeds. */
    public void testCommitIOExceptionEngineStaysOpen() throws Exception {
        createCompositeIndex();
        indexDocs(5);

        FailureInjectingCommitterPlugin.setCommitFailure(() -> new IOException("No space left on device"));

        try {
            flush();
        } catch (Exception e) {
            // expected — FlushFailedEngineException
        }

        // Engine should still be open — verify by indexing
        FailureInjectingCommitterPlugin.clearFailure();
        indexDocs(3);
        // Retry flush should succeed
        flush();
        assertEquals(7, getEngine().getSeqNoStats(-1).getMaxSeqNo());
    }

    /** Commit fails, clear failure, new flush succeeds with all data. */
    public void testCommitFailureThenRecovery() throws Exception {
        createCompositeIndex();
        indexDocs(5);
        flush();

        indexDocs(5);

        FailureInjectingCommitterPlugin.setCommitFailure(() -> new IOException("disk full"));
        try {
            flush();
        } catch (Exception e) {
            // expected
        }

        FailureInjectingCommitterPlugin.clearFailure();
        indexDocs(2);
        flush();
        // 5 + 5 + 2 = 12 ops
        assertEquals(11, getEngine().getSeqNoStats(-1).getMaxSeqNo());
    }

    // --- Helpers ---

    private void createCompositeIndex() {
        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.pluggable.dataformat.enabled", true)
                .put("index.pluggable.dataformat", "composite")
                .put("index.composite.primary_data_format", FileBackedDataFormatPlugin.FORMAT_NAME)
                .putList("index.composite.secondary_data_formats")
                .build()
        );
        ensureGreen(INDEX_NAME);
    }

    private DataFormatAwareEngine getEngine() {
        String nodeId = clusterService().state().routingTable().index(INDEX_NAME).shard(0).primaryShard().currentNodeId();
        String nodeName = clusterService().state().nodes().get(nodeId).getName();
        IndexService svc = internalCluster().getInstance(IndicesService.class, nodeName).indexServiceSafe(resolveIndex(INDEX_NAME));
        return (DataFormatAwareEngine) IndexShardTestCase.getIndexer(svc.getShard(0));
    }

    private void indexDocs(int count) {
        for (int i = 0; i < count; i++) {
            assertEquals(RestStatus.CREATED, client().prepareIndex(INDEX_NAME).setSource("field", "v" + i).get().status());
        }
    }

    private void flush() {
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();
    }

    // --- FailureInjectingCommitterPlugin ---

    public static class FailureInjectingCommitterPlugin extends Plugin implements EnginePlugin {
        private static final AtomicReference<Supplier<Exception>> commitFailure = new AtomicReference<>();

        public static void setCommitFailure(Supplier<Exception> failure) {
            commitFailure.set(failure);
        }

        public static void clearFailure() {
            commitFailure.set(null);
        }

        @Override
        public Optional<CommitterFactory> getCommitterFactory(org.opensearch.index.IndexSettings indexSettings) {
            return Optional.of(config -> {
                InMemoryCommitter committer = new InMemoryCommitter(config.engineConfig().getStore());
                // Wire the static failure supplier so it's checked on every commit
                committer.setCommitFailure(() -> {
                    Supplier<Exception> failure = commitFailure.get();
                    return failure != null ? failure.get() : null;
                });
                return committer;
            });
        }
    }
}
