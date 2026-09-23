/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.apache.logging.log4j.LogManager;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.node.Node;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.containsString;

/**
 * Unit tests for {@link IndexService#validateTieredRecoverySettings}, the shard-creation fail-fast for
 * {@code index.remote_store.tiered_recovery.enabled}. Exercised directly because {@link IndexServiceTests} runs on a
 * single non-remote-store node and cannot create the remote-store index the happy path needs.
 */
public class TieredRecoveryValidationTests extends OpenSearchTestCase {

    private static final String TIERED_KEY = IndexModule.INDEX_REMOTE_STORE_TIERED_RECOVERY_ENABLED_SETTING.getKey();
    private static final ShardId SHARD_ID = new ShardId(new Index("index", "_na_"), 0);
    private static final Settings HYDRATION_CACHE_NODE = Settings.builder()
        .put(Node.NODE_REMOTE_STORE_HYDRATION_CACHE_SIZE_SETTING.getKey(), "1gb")
        .build();

    private static Settings.Builder remoteStoreIndex() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
            .put(TIERED_KEY, true);
    }

    private static IndexSettings indexSettings(Settings.Builder index, Settings node) {
        return IndexSettingsModule.newIndexSettings(SHARD_ID.getIndex(), index.build(), node);
    }

    private static void validate(IndexSettings settings) {
        IndexService.validateTieredRecoverySettings(settings, SHARD_ID, LogManager.getLogger(TieredRecoveryValidationTests.class));
    }

    public void testNoOpWhenNotOptedIn() {
        // no feature flag, no remote store, no cache: nothing is validated when the index did not opt in
        validate(indexSettings(Settings.builder(), Settings.EMPTY));
    }

    public void testRejectsWhenFeatureFlagDisabled() {
        assumeFalse("flag must be off for this case", FeatureFlags.isEnabled(FeatureFlags.WRITABLE_WARM_INDEX_SETTING));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> validate(indexSettings(remoteStoreIndex(), HYDRATION_CACHE_NODE))
        );
        assertThat(e.getMessage(), containsString(TIERED_KEY));
        assertThat(e.getMessage(), containsString(FeatureFlags.WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG));
    }

    @LockFeatureFlag(FeatureFlags.WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG)
    public void testRejectsNonRemoteStoreIndex() {
        Settings.Builder index = Settings.builder().put(TIERED_KEY, true);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> validate(indexSettings(index, HYDRATION_CACHE_NODE))
        );
        assertThat(e.getMessage(), containsString("remote store enabled index"));
    }

    @LockFeatureFlag(FeatureFlags.WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG)
    public void testRejectsWarmIndex() {
        Settings.Builder index = remoteStoreIndex().put(IndexModule.IS_WARM_INDEX_SETTING.getKey(), true);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> validate(indexSettings(index, HYDRATION_CACHE_NODE))
        );
        assertThat(e.getMessage(), containsString(IndexModule.IS_WARM_INDEX_SETTING.getKey()));
    }

    @LockFeatureFlag(FeatureFlags.WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG)
    public void testRejectsNodeWithoutHydrationCache() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> validate(indexSettings(remoteStoreIndex(), Settings.EMPTY))
        );
        assertThat(e.getMessage(), containsString(Node.NODE_REMOTE_STORE_HYDRATION_CACHE_SIZE_SETTING.getKey()));
        assertThat(e.getMessage(), containsString(SHARD_ID.toString()));
    }

    @LockFeatureFlag(FeatureFlags.WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG)
    public void testAcceptsRemoteStoreHotIndexWithHydrationCache() {
        validate(indexSettings(remoteStoreIndex(), HYDRATION_CACHE_NODE));
    }

    public void testPluggableDataFormatIndexIsAcceptedNotRejected() throws Exception {
        // Track A: data-format-aware indices keep the full-download path and are only logged, never rejected
        FeatureFlags.TestUtils.with(
            FeatureFlags.WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG,
            () -> FeatureFlags.TestUtils.with(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, () -> {
                Settings.Builder index = remoteStoreIndex().put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true);
                IndexSettings settings = indexSettings(index, HYDRATION_CACHE_NODE);
                assertTrue(settings.isPluggableDataFormatEnabled());
                validate(settings);
            })
        );
    }
}
