/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.IndexModule;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.node.Node;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Path;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class WarmIndexRemoteStoreSegmentReplicationIT extends SegmentReplicationIT {

    protected static final String REPOSITORY_NAME = "test-remote-store-repo";
    protected Path absolutePath;

    @Before
    private void setup() {
        internalCluster().startClusterManagerOnlyNode();
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexModule.INDEX_STORE_LOCALITY_SETTING.getKey(), IndexModule.DataLocalityType.PARTIAL.name())
            .build();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        if (absolutePath == null) {
            absolutePath = randomRepoPath().toAbsolutePath();
        }
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(remoteStoreClusterSettings(REPOSITORY_NAME, absolutePath))
            .build();
    }

    @Override
    protected Settings featureFlagSettings() {
        Settings.Builder featureSettings = Settings.builder();
        featureSettings.put(FeatureFlags.TIERED_REMOTE_INDEX, true);

        return featureSettings.build();
    }

    @Override
    protected boolean addMockIndexStorePlugin() {
        return false;
    }

    protected boolean warmIndexSegmentReplicationEnabled() {
        return true;
    }

    @After
    public void teardown() {
        for (String nodeName : internalCluster().getNodeNames()) {
            logger.info("file cache node name is {}", nodeName);
            FileCache fileCache = internalCluster().getInstance(Node.class, nodeName).fileCache();
            fileCache.clear();
        }
        clusterAdmin().prepareCleanupRepository(REPOSITORY_NAME).get();
    }
}
