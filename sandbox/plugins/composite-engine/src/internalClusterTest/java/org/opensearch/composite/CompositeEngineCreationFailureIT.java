/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.engine.dataformat.stub.FileBackedDataFormatPlugin;
import org.opensearch.index.engine.dataformat.stub.InMemoryCommitter;
import org.opensearch.index.engine.exec.commit.CommitterFactory;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Verifies that persistent engine creation failure (e.g., disk full during committer init)
 * causes the shard to go UNASSIGNED after max allocation retries, not loop endlessly.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class CompositeEngineCreationFailureIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "test-creation-failure";
    private static final AtomicBoolean failOnCreation = new AtomicBoolean(false);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(FileBackedDataFormatPlugin.class, CompositeDataFormatPlugin.class, FailOnCreationCommitterPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .build();
    }

    @Override
    public void setUp() throws Exception {
        failOnCreation.set(false);
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        failOnCreation.set(false);
        super.tearDown();
    }

    public void testEngineCreationFailureShardGoesUnassigned() throws Exception {
        failOnCreation.set(true);

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

        // Shard should go UNASSIGNED after max retries — not loop endlessly
        assertBusy(() -> {
            ShardRouting primary = clusterService().state().routingTable().index(INDEX_NAME).shard(0).primaryShard();
            assertTrue("shard should be unassigned after persistent creation failure", primary.unassigned());
        }, 2, java.util.concurrent.TimeUnit.MINUTES);
    }

    public static class FailOnCreationCommitterPlugin extends Plugin implements EnginePlugin {
        @Override
        public Optional<CommitterFactory> getCommitterFactory(org.opensearch.index.IndexSettings indexSettings) {
            return Optional.of(config -> {
                if (failOnCreation.get()) {
                    throw new IOException("simulated engine creation failure (disk full)");
                }
                return new InMemoryCommitter(config.engineConfig().getStore());
            });
        }
    }
}
