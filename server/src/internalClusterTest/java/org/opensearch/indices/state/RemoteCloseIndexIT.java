/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.state;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.node.Node;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;

import static org.opensearch.common.util.FeatureFlags.WRITABLE_WARM_INDEX_SETTING;

public class RemoteCloseIndexIT extends CloseIndexIT {

    public RemoteCloseIndexIT(Settings nodeSettings) {
        super(nodeSettings);
    }

    protected Path remoteRepoPath;

    protected final static String TEST_REMOTE_STORE_REPO_SUFFIX = "__rs";
    protected static final String BASE_REMOTE_REPO = "test-rs-repo" + TEST_REMOTE_STORE_REPO_SUFFIX;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        if (remoteRepoPath == null) {
            remoteRepoPath = randomRepoPath().toAbsolutePath();
        }
        ByteSizeValue cacheSize = new ByteSizeValue(16, ByteSizeUnit.GB);
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(remoteStoreClusterSettings(BASE_REMOTE_REPO, remoteRepoPath))
            .put(Node.NODE_SEARCH_CACHE_SIZE_SETTING.getKey(), cacheSize.toString())
            .build();
    }

    protected Settings.Builder getIndexSettings(int numOfShards, int numOfReplicas) {
        Settings.Builder settingsBuilder = Settings.builder()
            .put(super.indexSettings())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numOfShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numOfReplicas)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "300s");
        if (WRITABLE_WARM_INDEX_SETTING.get(settings)) {
            settingsBuilder.put(IndexModule.IS_WARM_INDEX_SETTING.getKey(), true);
        }
        return settingsBuilder;
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(WRITABLE_WARM_INDEX_SETTING.getKey(), true).build() },
            new Object[] { Settings.builder().put(WRITABLE_WARM_INDEX_SETTING.getKey(), false).build() }
        );
    }

    void assertNoFileBasedRecovery(String indexName) {
        // skipping for remote store
    }
}
