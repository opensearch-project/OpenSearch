/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.filter.RegexFilter;
import org.opensearch.Version;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.merge.MergedSegmentTransferTracker;
import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.opensearch.index.IndexSettingsTests.newIndexMeta;
import static org.opensearch.index.MergeSchedulerConfig.CLUSTER_MAX_FORCE_MERGE_MB_PER_SEC_SETTING;
import static org.opensearch.index.MergeSchedulerConfig.MAX_FORCE_MERGE_MB_PER_SEC_SETTING;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for merge rate limiting functionality in OpenSearchConcurrentMergeScheduler
 */
public class MergeRateLimitingTests extends OpenSearchTestCase {

    private static class MockAppender extends AbstractAppender {
        public boolean sawRateLimitUpdate;
        public String lastRateLimitMessage;

        MockAppender(final String name) throws IllegalAccessException {
            super(name, RegexFilter.createFilter(".*(\n.*)*", new String[0], false, null, null), null);
        }

        @Override
        public void append(LogEvent event) {
            String message = event.getMessage().getFormattedMessage();
            if (event.getLevel() == Level.INFO && message.contains("updating force merge rate limit")) {
                sawRateLimitUpdate = true;
                lastRateLimitMessage = message;
            }
        }

        @Override
        public boolean ignoreExceptions() {
            return false;
        }

        public void reset() {
            sawRateLimitUpdate = false;
            lastRateLimitMessage = null;
        }
    }

    /**
     * Test that index-level settings take precedence over cluster-level settings
     */
    public void testSettingPrecedence() {
        // Test with only cluster-level setting
        Settings nodeSettings = Settings.builder().put(CLUSTER_MAX_FORCE_MERGE_MB_PER_SEC_SETTING.getKey(), "75.0").build();

        Settings.Builder indexBuilder = Settings.builder()
            .put(SETTING_VERSION_CREATED, Version.CURRENT)
            .put(SETTING_NUMBER_OF_SHARDS, "1")
            .put(SETTING_NUMBER_OF_REPLICAS, "0");

        IndexSettings indexSettings = new IndexSettings(newIndexMeta("test_index", indexBuilder.build()), nodeSettings);
        ShardId shardId = new ShardId("test_index", "test_uuid", 0);

        OpenSearchConcurrentMergeScheduler scheduler = new OpenSearchConcurrentMergeScheduler(
            shardId,
            indexSettings,
            new MergedSegmentTransferTracker()
        );

        // Should use cluster-level setting
        assertThat(scheduler.getForceMergeMBPerSec(), equalTo(75.0));

        // Test with both index and cluster-level settings - index should take precedence
        indexBuilder.put(MAX_FORCE_MERGE_MB_PER_SEC_SETTING.getKey(), "25.0");
        indexSettings = new IndexSettings(newIndexMeta("test_index", indexBuilder.build()), nodeSettings);
        scheduler = new OpenSearchConcurrentMergeScheduler(shardId, indexSettings, new MergedSegmentTransferTracker());

        // Should use index-level setting
        assertThat(scheduler.getForceMergeMBPerSec(), equalTo(25.0));
    }

    /**
     * Test that disabled rate limiting (Double.POSITIVE_INFINITY) works correctly
     */
    public void testDisabledRateLimiting() {
        Settings.Builder builder = Settings.builder()
            .put(SETTING_VERSION_CREATED, Version.CURRENT)
            .put(SETTING_NUMBER_OF_SHARDS, "1")
            .put(SETTING_NUMBER_OF_REPLICAS, "0")
            .put(MAX_FORCE_MERGE_MB_PER_SEC_SETTING.getKey(), Double.POSITIVE_INFINITY);

        IndexSettings indexSettings = new IndexSettings(newIndexMeta("test_index", builder.build()), Settings.EMPTY);
        ShardId shardId = new ShardId("test_index", "test_uuid", 0);

        OpenSearchConcurrentMergeScheduler scheduler = new OpenSearchConcurrentMergeScheduler(
            shardId,
            indexSettings,
            new MergedSegmentTransferTracker()
        );

        // Should have no rate limiting
        assertThat(scheduler.getForceMergeMBPerSec(), equalTo(Double.POSITIVE_INFINITY));
    }

    /**
     * Test that rate limiting configuration changes are applied when scheduler is refreshed
     */
    public void testDynamicRateLimitUpdates() throws Exception {
        MockAppender mockAppender = new MockAppender("testDynamicRateLimitUpdates");
        mockAppender.start();
        final Logger logger = LogManager.getLogger(OpenSearchConcurrentMergeScheduler.class);
        Loggers.addAppender(logger, mockAppender);
        Loggers.setLevel(logger, Level.INFO);

        try {
            Settings.Builder builder = Settings.builder()
                .put(SETTING_VERSION_CREATED, Version.CURRENT)
                .put(SETTING_NUMBER_OF_SHARDS, "1")
                .put(SETTING_NUMBER_OF_REPLICAS, "0")
                .put(MAX_FORCE_MERGE_MB_PER_SEC_SETTING.getKey(), "10.0");

            IndexSettings indexSettings = new IndexSettings(newIndexMeta("test_index", builder.build()), Settings.EMPTY);
            ShardId shardId = new ShardId("test_index", "test_uuid", 0);

            OpenSearchConcurrentMergeScheduler scheduler = new OpenSearchConcurrentMergeScheduler(
                shardId,
                indexSettings,
                new MergedSegmentTransferTracker()
            );
            assertThat(scheduler.getForceMergeMBPerSec(), equalTo(10.0));

            // Update to a different rate limit
            mockAppender.reset();
            builder.put(MAX_FORCE_MERGE_MB_PER_SEC_SETTING.getKey(), "20.0");
            indexSettings.updateIndexMetadata(newIndexMeta("test_index", builder.build()));
            scheduler.refreshConfig();

            assertThat(scheduler.getForceMergeMBPerSec(), equalTo(20.0));
            assertTrue("Should log rate limit update", mockAppender.sawRateLimitUpdate);

            // Update to disable rate limiting
            mockAppender.reset();
            builder.put(MAX_FORCE_MERGE_MB_PER_SEC_SETTING.getKey(), Double.POSITIVE_INFINITY);
            indexSettings.updateIndexMetadata(newIndexMeta("test_index", builder.build()));
            scheduler.refreshConfig();

            assertThat(scheduler.getForceMergeMBPerSec(), equalTo(Double.POSITIVE_INFINITY));
            assertTrue("Should log rate limit update", mockAppender.sawRateLimitUpdate);

            // Re-enable with a new rate
            mockAppender.reset();
            builder.put(MAX_FORCE_MERGE_MB_PER_SEC_SETTING.getKey(), "30.0");
            indexSettings.updateIndexMetadata(newIndexMeta("test_index", builder.build()));
            scheduler.refreshConfig();

            assertThat(scheduler.getForceMergeMBPerSec(), equalTo(30.0));
            assertTrue("Should log rate limit update", mockAppender.sawRateLimitUpdate);

        } finally {
            Loggers.removeAppender(logger, mockAppender);
            mockAppender.stop();
            Loggers.setLevel(logger, (Level) null);
        }
    }

    /**
     * Test that when index-level setting is removed, scheduler falls back to cluster-level setting
     */
    public void testFallbackToClusterSettingWhenIndexSettingRemoved() throws Exception {
        MockAppender mockAppender = new MockAppender("testFallbackToClusterSettingWhenIndexSettingRemoved");
        mockAppender.start();
        final Logger logger = LogManager.getLogger(OpenSearchConcurrentMergeScheduler.class);
        Loggers.addAppender(logger, mockAppender);
        Loggers.setLevel(logger, Level.INFO);

        try {
            Settings nodeSettings = Settings.builder().put(CLUSTER_MAX_FORCE_MERGE_MB_PER_SEC_SETTING.getKey(), "50.0").build();

            // Start with index-level setting that overrides cluster setting
            Settings.Builder builder = Settings.builder()
                .put(SETTING_VERSION_CREATED, Version.CURRENT)
                .put(SETTING_NUMBER_OF_SHARDS, "1")
                .put(SETTING_NUMBER_OF_REPLICAS, "0")
                .put(MAX_FORCE_MERGE_MB_PER_SEC_SETTING.getKey(), "25.0");

            IndexSettings indexSettings = new IndexSettings(newIndexMeta("test_index", builder.build()), nodeSettings);
            ShardId shardId = new ShardId("test_index", "test_uuid", 0);

            OpenSearchConcurrentMergeScheduler scheduler = new OpenSearchConcurrentMergeScheduler(
                shardId,
                indexSettings,
                new MergedSegmentTransferTracker()
            );

            // Should initially use index-level setting
            assertThat(scheduler.getForceMergeMBPerSec(), equalTo(25.0));

            // Remove the index-level setting by NOT setting MAX_FORCE_MERGE_MB_PER_SEC_SETTING - should trigger fallback to cluster setting
            mockAppender.reset();
            Settings.Builder newBuilder = Settings.builder()
                .put(SETTING_VERSION_CREATED, Version.CURRENT)
                .put(SETTING_NUMBER_OF_SHARDS, "1")
                .put(SETTING_NUMBER_OF_REPLICAS, "0");

            indexSettings.updateIndexMetadata(newIndexMeta("test_index", newBuilder.build()));
            scheduler.refreshConfig();

            // Should now use cluster-level setting
            assertThat(scheduler.getForceMergeMBPerSec(), equalTo(50.0));
            assertTrue("Should log rate limit update when falling back to cluster setting", mockAppender.sawRateLimitUpdate);

        } finally {
            Loggers.removeAppender(logger, mockAppender);
            mockAppender.stop();
            Loggers.setLevel(logger, (Level) null);
        }
    }
}
