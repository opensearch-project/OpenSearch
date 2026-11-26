/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.recovery.IndexPrimaryRelocationIT;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

import static org.opensearch.remotestore.RemoteStoreBaseIntegTestCase.segmentsOnlyNodeAttributes;

/**
 * Handing a primary over while writes keep arriving is where a local translog and remote segments part company: the
 * operations the source acknowledged after its last upload exist only in its translog, so they have to be shipped
 * rather than left to the remote store. The inherited test relocates fifteen times under a writer thread.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SegmentsOnlyPrimaryRelocationIT extends IndexPrimaryRelocationIT {

    private static final String REPOSITORY_NAME = "test-segment-repo";

    private Path absolutePath;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(InternalSettingsPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        if (absolutePath == null) {
            absolutePath = randomRepoPath().toAbsolutePath();
        }
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(segmentsOnlyNodeAttributes(REPOSITORY_NAME, absolutePath))
            .build();
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
    }

    @Override
    public void testPrimaryRelocationWhileIndexing() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        super.testPrimaryRelocationWhileIndexing();
    }
}
