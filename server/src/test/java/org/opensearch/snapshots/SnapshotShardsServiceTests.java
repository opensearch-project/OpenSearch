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
import org.opensearch.index.IndexSettings;
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

    private static IndexSettings indexSettings(boolean pluggableDataFormat) {
        return IndexSettingsModule.newIndexSettings(
            "test-index",
            Settings.builder()
                .put(IndexMetadata.SETTING_INDEX_UUID, "test-uuid")
                .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), pluggableDataFormat)
                .build()
        );
    }

    /**
     * The catalog-based snapshot path is not validated for multi-format shards yet, so it must be refused at the
     * API boundary with a message that names the setting responsible.
     */
    @LockFeatureFlag(PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testPluggableDataFormatSnapshotIsRejected() {
        final IndexSettings settings = indexSettings(true);
        assertTrue("guard should only fire for pluggable data format indices", settings.isPluggableDataFormatEnabled());

        final IndexShardSnapshotFailedException e = expectThrows(
            IndexShardSnapshotFailedException.class,
            () -> SnapshotShardsService.ensurePluggableDataFormatSnapshotSupported(TEST_SHARD_ID, settings)
        );
        assertThat(e.getMessage(), containsString("snapshots are not yet supported"));
        assertThat(e.getMessage(), containsString(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey()));
    }

    /** The guard must not change behaviour for any index that does not use a pluggable data format. */
    @LockFeatureFlag(PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testNonPluggableDataFormatIndexIsAllowed() {
        final IndexSettings settings = indexSettings(false);
        assertFalse(settings.isPluggableDataFormatEnabled());
        SnapshotShardsService.ensurePluggableDataFormatSnapshotSupported(TEST_SHARD_ID, settings);
    }

    /**
     * Without the experimental feature flag the index setting is inert, so the guard must stay out of the way even
     * when the setting is present in the index metadata.
     */
    public void testGuardIsInertWhenFeatureFlagIsDisabled() {
        final IndexSettings settings = indexSettings(true);
        assertFalse("feature flag is off, so the index setting must not take effect", settings.isPluggableDataFormatEnabled());
        SnapshotShardsService.ensurePluggableDataFormatSnapshotSupported(TEST_SHARD_ID, settings);
    }
}
