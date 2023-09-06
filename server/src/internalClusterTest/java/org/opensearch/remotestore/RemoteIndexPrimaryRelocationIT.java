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
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.indices.recovery.IndexPrimaryRelocationIT;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.OpenSearchIntegTestCase;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
public class RemoteIndexPrimaryRelocationIT extends IndexPrimaryRelocationIT {

    protected static final String REPOSITORY_NAME = "test-remote-store-repo";

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
    }

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder()
            .put(super.featureFlagSettings())
            .put(FeatureFlags.REMOTE_STORE, "true")
            .put(FeatureFlags.SEGMENT_REPLICATION_EXPERIMENTAL, "true")
            .build();
    }

    public void testPrimaryRelocationWhileIndexing() throws Exception {
        super.testPrimaryRelocationWhileIndexing();
    }
}
