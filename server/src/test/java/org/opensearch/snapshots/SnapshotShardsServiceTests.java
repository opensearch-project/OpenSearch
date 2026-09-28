/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.snapshots;

import org.opensearch.cluster.SnapshotsInProgress;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.index.snapshots.IndexShardSnapshotFailedException;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.EqualsHashCodeTestUtils;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.opensearch.common.util.FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class SnapshotShardsServiceTests extends OpenSearchTestCase {

    public void testSummarizeFailure() {
        final RuntimeException wrapped = new RuntimeException("wrapped");
        assertThat(SnapshotShardsService.summarizeFailure(wrapped), is("RuntimeException[wrapped]"));
        final RuntimeException wrappedWithNested = new RuntimeException("wrapped", new IOException("nested"));
        assertThat(SnapshotShardsService.summarizeFailure(wrappedWithNested), is("RuntimeException[wrapped]; nested: IOException[nested]"));
        final RuntimeException wrappedWithTwoNested = new RuntimeException("wrapped", new IOException("nested", new IOException("root")));
        assertThat(
            SnapshotShardsService.summarizeFailure(wrappedWithTwoNested),
            is("RuntimeException[wrapped]; nested: IOException[nested]; nested: IOException[root]")
        );
    }

    public void testEqualsAndHashcodeUpdateIndexShardSnapshotStatusRequest() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            new UpdateIndexShardSnapshotStatusRequest(
                new Snapshot(randomAlphaOfLength(10), new SnapshotId(randomAlphaOfLength(10), UUIDs.randomBase64UUID(random()))),
                new ShardId(randomAlphaOfLength(10), UUIDs.randomBase64UUID(random()), randomInt(5)),
                new SnapshotsInProgress.ShardSnapshotStatus(randomAlphaOfLength(10), UUIDs.randomBase64UUID(random()))
            ),
            request -> new UpdateIndexShardSnapshotStatusRequest(request.snapshot(), request.shardId(), request.status()),
            request -> {
                final boolean mutateSnapshot = randomBoolean();
                final boolean mutateShardId = randomBoolean();
                final boolean mutateStatus = (mutateSnapshot || mutateShardId) == false || randomBoolean();
                return new UpdateIndexShardSnapshotStatusRequest(
                    mutateSnapshot
                        ? new Snapshot(randomAlphaOfLength(10), new SnapshotId(randomAlphaOfLength(10), UUIDs.randomBase64UUID(random())))
                        : request.snapshot(),
                    mutateShardId
                        ? new ShardId(randomAlphaOfLength(10), UUIDs.randomBase64UUID(random()), randomInt(5))
                        : request.shardId(),
                    mutateStatus
                        ? new SnapshotsInProgress.ShardSnapshotStatus(randomAlphaOfLength(10), UUIDs.randomBase64UUID(random()))
                        : request.status()
                );
            }
        );
    }

    private static final ShardId TEST_SHARD_ID = new ShardId("test-index", "test-uuid", 0);

    private static IndexSettings indexSettings(boolean pluggableDataFormat, boolean remoteStore, boolean warm) {
        Settings.Builder builder = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_UUID, "test-uuid")
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), pluggableDataFormat)
            .put(IndexModule.IS_WARM_INDEX_SETTING.getKey(), warm);
        if (remoteStore) {
            // index.remote_store.enabled is only valid alongside segment replication
            builder.put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT);
        }
        return IndexSettingsModule.newIndexSettings("test-index", builder.build());
    }

    /**
     * Full-copy snapshots must be allowed for pluggable data format shards: that is the path Phase 1/2 wire up.
     */
    @LockFeatureFlag(PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testPluggableDataFormatFullCopySnapshotIsAllowed() {
        IndexSettings settings = indexSettings(true, true, false);
        assertTrue("guard should only fire for pluggable data format indices", settings.isPluggableDataFormatEnabled());
        // no exception expected
        SnapshotShardsService.ensurePluggableDataFormatSnapshotSupported(TEST_SHARD_ID, settings, false);
    }

    /**
     * The shallow-copy path has no catalog-aware lock resolution or restore path yet, so it must be rejected
     * with a message that names the repository setting to flip.
     */
    @LockFeatureFlag(PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testPluggableDataFormatShallowCopySnapshotIsRejected() {
        IndexShardSnapshotFailedException e = expectThrows(
            IndexShardSnapshotFailedException.class,
            () -> SnapshotShardsService.ensurePluggableDataFormatSnapshotSupported(TEST_SHARD_ID, indexSettings(true, true, false), true)
        );
        assertThat(e.getMessage(), containsString("shallow copy snapshots are not supported"));
        assertThat(e.getMessage(), containsString(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey()));
        assertThat(e.getMessage(), containsString(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey()));
    }

    /**
     * {@code remoteStoreIndexShallowCopy=true} on a repository only selects the shallow path when the index is
     * itself remote-store backed, so a non-remote-store index still takes the (allowed) full-copy path.
     */
    @LockFeatureFlag(PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testPluggableDataFormatShallowCopyRepositoryWithoutRemoteStoreIsAllowed() {
        SnapshotShardsService.ensurePluggableDataFormatSnapshotSupported(TEST_SHARD_ID, indexSettings(true, false, false), true);
    }

    @LockFeatureFlag(PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testPluggableDataFormatWarmIndexSnapshotIsRejected() {
        IndexSettings settings = indexSettings(true, false, true);
        assertTrue(settings.isWarmIndex());
        IndexShardSnapshotFailedException e = expectThrows(
            IndexShardSnapshotFailedException.class,
            () -> SnapshotShardsService.ensurePluggableDataFormatSnapshotSupported(TEST_SHARD_ID, settings, false)
        );
        assertThat(e.getMessage(), containsString("warm indices"));
        assertThat(e.getMessage(), containsString(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey()));
    }

    /**
     * The guard must not change behaviour for any index that does not use a pluggable data format, including
     * the shallow-copy and warm combinations it rejects for those that do.
     */
    public void testNonPluggableDataFormatIndicesAreNeverRejected() {
        for (boolean shallowCopy : new boolean[] { true, false }) {
            for (boolean warm : new boolean[] { true, false }) {
                IndexSettings settings = indexSettings(false, true, warm);
                assertFalse(settings.isPluggableDataFormatEnabled());
                SnapshotShardsService.ensurePluggableDataFormatSnapshotSupported(TEST_SHARD_ID, settings, shallowCopy);
            }
        }
    }

    /**
     * Without the experimental feature flag the index setting is inert, so the guard must stay out of the way
     * even when the setting is present in the index metadata.
     */
    public void testGuardIsInertWhenFeatureFlagIsDisabled() {
        IndexSettings settings = indexSettings(true, true, true);
        assertFalse("feature flag is off, so the index setting must not take effect", settings.isPluggableDataFormatEnabled());
        SnapshotShardsService.ensurePluggableDataFormatSnapshotSupported(TEST_SHARD_ID, settings, true);
    }
}
