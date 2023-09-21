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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.index;

import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.opensearch.common.settings.Settings.Builder.EMPTY_SETTINGS;
import static org.opensearch.index.IndexSettingsTests.newIndexMeta;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class MergePolicySettingsTests extends OpenSearchTestCase {
    protected final ShardId shardId = new ShardId("index", "_na_", 1);

    public void testCompoundFileSettings() throws IOException {
        assertThat(new TieredMergePolicyProvider(logger, indexSettings(Settings.EMPTY)).getMergePolicy().getNoCFSRatio(), equalTo(0.1));
        assertThat(new TieredMergePolicyProvider(logger, indexSettings(build(true))).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new TieredMergePolicyProvider(logger, indexSettings(build(0.5))).getMergePolicy().getNoCFSRatio(), equalTo(0.5));
        assertThat(new TieredMergePolicyProvider(logger, indexSettings(build(1.0))).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new TieredMergePolicyProvider(logger, indexSettings(build("true"))).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new TieredMergePolicyProvider(logger, indexSettings(build("True"))).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new TieredMergePolicyProvider(logger, indexSettings(build("False"))).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new TieredMergePolicyProvider(logger, indexSettings(build("false"))).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new TieredMergePolicyProvider(logger, indexSettings(build(false))).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new TieredMergePolicyProvider(logger, indexSettings(build(0))).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new TieredMergePolicyProvider(logger, indexSettings(build(0.0))).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
    }

    private static IndexSettings indexSettings(Settings settings) {
        return new IndexSettings(newIndexMeta("test", settings), Settings.EMPTY);
    }

    public void testNoMerges() {
        TieredMergePolicyProvider tmp = new TieredMergePolicyProvider(
            logger,
            indexSettings(Settings.builder().put(MergePolicyProvider.INDEX_MERGE_ENABLED, false).build())
        );
        LogByteSizeMergePolicyProvider lbsmp = new LogByteSizeMergePolicyProvider(
            logger,
            indexSettings(Settings.builder().put(MergePolicyProvider.INDEX_MERGE_ENABLED, false).build())
        );
        assertTrue(tmp.getMergePolicy() instanceof NoMergePolicy);
        assertTrue(lbsmp.getMergePolicy() instanceof NoMergePolicy);
    }

    public void testUpdateSettings() throws IOException {
        Settings settings = Settings.builder().put(IndexSettings.INDEX_MERGE_POLICY.getKey(), IndexSettings.TIERED_MERGE_POLICY).build();
        IndexSettings indexSettings = indexSettings(settings);
        assertThat(indexSettings.getMergePolicy(false).getNoCFSRatio(), equalTo(0.1));
        indexSettings = indexSettings(build(0.9));
        assertThat((indexSettings.getMergePolicy(false)).getNoCFSRatio(), equalTo(0.9));
        indexSettings.updateIndexMetadata(newIndexMeta("index", build(0.1)));
        assertThat((indexSettings.getMergePolicy(false)).getNoCFSRatio(), equalTo(0.1));
        indexSettings.updateIndexMetadata(newIndexMeta("index", build(0.0)));
        assertThat((indexSettings.getMergePolicy(false)).getNoCFSRatio(), equalTo(0.0));
        indexSettings.updateIndexMetadata(newIndexMeta("index", build("true")));
        assertThat((indexSettings.getMergePolicy(false)).getNoCFSRatio(), equalTo(1.0));
        indexSettings.updateIndexMetadata(newIndexMeta("index", build("false")));
        assertThat((indexSettings.getMergePolicy(false)).getNoCFSRatio(), equalTo(0.0));
    }

    public void testDefaultMergePolicy() throws IOException {
        IndexSettings indexSettings = indexSettings(EMPTY_SETTINGS);
        assertTrue(indexSettings.getMergePolicy(false) instanceof OpenSearchTieredMergePolicy);
        assertTrue(indexSettings.getMergePolicy(true) instanceof OpenSearchTieredMergePolicy);
    }

    public void testMergePolicyPrecedence() throws IOException {
        // 1. INDEX_MERGE_POLICY is not set
        // assert defaults
        IndexSettings indexSettings = indexSettings(EMPTY_SETTINGS);
        assertTrue(indexSettings.getMergePolicy(false) instanceof OpenSearchTieredMergePolicy);
        assertTrue(indexSettings.getMergePolicy(true) instanceof OpenSearchTieredMergePolicy);

        // 1.1 node setting TIME_INDEX_MERGE_POLICY is set as log_byte_size
        // assert index policy is tiered whereas time index policy is log_byte_size
        Settings nodeSettings = Settings.builder()
            .put(IndexSettings.TIME_INDEX_MERGE_POLICY.getKey(), IndexSettings.LOG_BYTE_SIZE_MERGE_POLICY)
            .build();
        indexSettings = new IndexSettings(newIndexMeta("test", Settings.EMPTY), nodeSettings);
        assertTrue(indexSettings.getMergePolicy(false) instanceof OpenSearchTieredMergePolicy);
        assertTrue(indexSettings.getMergePolicy(true) instanceof LogByteSizeMergePolicy);

        // 1.2 node setting TIME_INDEX_MERGE_POLICY is set as tiered
        // assert both index and time index policy is tiered
        nodeSettings = Settings.builder().put(IndexSettings.TIME_INDEX_MERGE_POLICY.getKey(), IndexSettings.TIERED_MERGE_POLICY).build();
        indexSettings = new IndexSettings(newIndexMeta("test", Settings.EMPTY), nodeSettings);
        assertTrue(indexSettings.getMergePolicy(false) instanceof OpenSearchTieredMergePolicy);
        assertTrue(indexSettings.getMergePolicy(true) instanceof OpenSearchTieredMergePolicy);

        // 2. INDEX_MERGE_POLICY set as tiered
        // assert both index and time-index merge policy is set as tiered
        indexSettings = indexSettings(
            Settings.builder().put(IndexSettings.INDEX_MERGE_POLICY.getKey(), IndexSettings.TIERED_MERGE_POLICY).build()
        );
        assertTrue(indexSettings.getMergePolicy(false) instanceof OpenSearchTieredMergePolicy);
        assertTrue(indexSettings.getMergePolicy(true) instanceof OpenSearchTieredMergePolicy);

        // 2.1 node setting TIME_INDEX_MERGE_POLICY is set as log_byte_size
        // assert both index and time-index merge policy is set as tiered
        nodeSettings = Settings.builder()
            .put(IndexSettings.TIME_INDEX_MERGE_POLICY.getKey(), IndexSettings.LOG_BYTE_SIZE_MERGE_POLICY)
            .build();
        indexSettings = new IndexSettings(
            newIndexMeta(
                "test",
                Settings.builder().put(IndexSettings.INDEX_MERGE_POLICY.getKey(), IndexSettings.TIERED_MERGE_POLICY).build()
            ),
            nodeSettings
        );
        assertTrue(indexSettings.getMergePolicy(false) instanceof OpenSearchTieredMergePolicy);
        assertTrue(indexSettings.getMergePolicy(true) instanceof OpenSearchTieredMergePolicy);

        // 3. INDEX_MERGE_POLICY set as log_byte_size
        // assert both index and time-index merge policy is set as log_byte_size
        indexSettings = indexSettings(
            Settings.builder().put(IndexSettings.INDEX_MERGE_POLICY.getKey(), IndexSettings.LOG_BYTE_SIZE_MERGE_POLICY).build()
        );
        assertTrue(indexSettings.getMergePolicy(false) instanceof LogByteSizeMergePolicy);
        assertTrue(indexSettings.getMergePolicy(true) instanceof LogByteSizeMergePolicy);

        // 3.1 node setting TIME_INDEX_MERGE_POLICY is set as tiered
        // assert both index and time-index merge policy is set as log_byte_size
        nodeSettings = Settings.builder().put(IndexSettings.TIME_INDEX_MERGE_POLICY.getKey(), IndexSettings.TIERED_MERGE_POLICY).build();
        indexSettings = new IndexSettings(
            newIndexMeta(
                "test",
                Settings.builder().put(IndexSettings.INDEX_MERGE_POLICY.getKey(), IndexSettings.LOG_BYTE_SIZE_MERGE_POLICY).build()
            ),
            nodeSettings
        );
        assertTrue(indexSettings.getMergePolicy(false) instanceof LogByteSizeMergePolicy);
        assertTrue(indexSettings.getMergePolicy(true) instanceof LogByteSizeMergePolicy);

    }

    public void testInvalidMergePolicy() throws IOException {

        final Settings invalidSettings = Settings.builder().put(IndexSettings.INDEX_MERGE_POLICY.getKey(), "invalid").build();
        IllegalArgumentException exc1 = expectThrows(
            IllegalArgumentException.class,
            () -> IndexSettings.INDEX_MERGE_POLICY.get(invalidSettings)
        );
        assertThat(exc1.getMessage(), containsString(IndexSettings.INDEX_MERGE_POLICY.getKey() + " has unsupported policy specified: "));
        IllegalArgumentException exc2 = expectThrows(
            IllegalArgumentException.class,
            () -> indexSettings(invalidSettings).getMergePolicy(false)
        );
        assertThat(exc2.getMessage(), containsString(IndexSettings.INDEX_MERGE_POLICY.getKey() + " has unsupported policy specified: "));

        final Settings invalidSettings2 = Settings.builder().put(IndexSettings.TIME_INDEX_MERGE_POLICY.getKey(), "invalid").build();
        IllegalArgumentException exc3 = expectThrows(
            IllegalArgumentException.class,
            () -> IndexSettings.TIME_INDEX_MERGE_POLICY.get(invalidSettings2)
        );
        assertThat(
            exc3.getMessage(),
            containsString(IndexSettings.TIME_INDEX_MERGE_POLICY.getKey() + " has unsupported policy specified: ")
        );

        IllegalArgumentException exc4 = expectThrows(
            IllegalArgumentException.class,
            () -> new IndexSettings(newIndexMeta("test", Settings.EMPTY), invalidSettings2).getMergePolicy(true)
        );
        assertThat(
            exc4.getMessage(),
            containsString(IndexSettings.TIME_INDEX_MERGE_POLICY.getKey() + " has unsupported policy specified: ")
        );
    }

    public void testUpdateSettingsForLogByteSizeMergePolicy() throws IOException {
        IndexSettings indexSettings = indexSettings(
            Settings.builder().put(IndexSettings.INDEX_MERGE_POLICY.getKey(), IndexSettings.LOG_BYTE_SIZE_MERGE_POLICY).build()
        );
        assertTrue(indexSettings.getMergePolicy(true) instanceof LogByteSizeMergePolicy);
        assertThat(indexSettings.getMergePolicy(true).getNoCFSRatio(), equalTo(0.1));
        indexSettings = indexSettings(
            Settings.builder()
                .put(IndexSettings.INDEX_MERGE_POLICY.getKey(), "log_byte_size")
                .put(LogByteSizeMergePolicyProvider.INDEX_LBS_NO_CFS_RATIO_SETTING.getKey(), 0.9)
                .build()
        );
        assertThat((indexSettings.getMergePolicy(true)).getNoCFSRatio(), equalTo(0.9));
        indexSettings = indexSettings(
            Settings.builder()
                .put(IndexSettings.INDEX_MERGE_POLICY.getKey(), "log_byte_size")
                .put(LogByteSizeMergePolicyProvider.INDEX_LBS_NO_CFS_RATIO_SETTING.getKey(), 0.1)
                .build()
        );
        assertThat((indexSettings.getMergePolicy(true)).getNoCFSRatio(), equalTo(0.1));
        indexSettings = indexSettings(
            Settings.builder()
                .put(IndexSettings.INDEX_MERGE_POLICY.getKey(), "log_byte_size")
                .put(LogByteSizeMergePolicyProvider.INDEX_LBS_NO_CFS_RATIO_SETTING.getKey(), 0.0)
                .build()
        );
        assertThat((indexSettings.getMergePolicy(true)).getNoCFSRatio(), equalTo(0.0));
        indexSettings = indexSettings(
            Settings.builder()
                .put(IndexSettings.INDEX_MERGE_POLICY.getKey(), "log_byte_size")
                .put(LogByteSizeMergePolicyProvider.INDEX_LBS_NO_CFS_RATIO_SETTING.getKey(), "true")
                .build()
        );
        assertThat((indexSettings.getMergePolicy(true)).getNoCFSRatio(), equalTo(1.0));
        indexSettings = indexSettings(
            Settings.builder()
                .put(IndexSettings.INDEX_MERGE_POLICY.getKey(), "log_byte_size")
                .put(LogByteSizeMergePolicyProvider.INDEX_LBS_NO_CFS_RATIO_SETTING.getKey(), "false")
                .build()
        );
        assertThat((indexSettings.getMergePolicy(true)).getNoCFSRatio(), equalTo(0.0));
    }

    public void testTieredMergePolicySettingsUpdate() throws IOException {
        IndexSettings indexSettings = indexSettings(Settings.EMPTY);
        assertEquals(
            ((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy(false)).getForceMergeDeletesPctAllowed(),
            TieredMergePolicyProvider.DEFAULT_EXPUNGE_DELETES_ALLOWED,
            0.0d
        );

        indexSettings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(
                        TieredMergePolicyProvider.INDEX_MERGE_POLICY_EXPUNGE_DELETES_ALLOWED_SETTING.getKey(),
                        TieredMergePolicyProvider.DEFAULT_EXPUNGE_DELETES_ALLOWED + 1.0d
                    )
                    .build()
            )
        );
        assertEquals(
            ((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy(false)).getForceMergeDeletesPctAllowed(),
            TieredMergePolicyProvider.DEFAULT_EXPUNGE_DELETES_ALLOWED + 1.0d,
            0.0d
        );

        assertEquals(
            ((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy(false)).getFloorSegmentMB(),
            TieredMergePolicyProvider.DEFAULT_FLOOR_SEGMENT.getMbFrac(),
            0
        );
        indexSettings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(
                        TieredMergePolicyProvider.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING.getKey(),
                        new ByteSizeValue(TieredMergePolicyProvider.DEFAULT_FLOOR_SEGMENT.getMb() + 1, ByteSizeUnit.MB)
                    )
                    .build()
            )
        );
        assertEquals(
            ((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy(false)).getFloorSegmentMB(),
            new ByteSizeValue(TieredMergePolicyProvider.DEFAULT_FLOOR_SEGMENT.getMb() + 1, ByteSizeUnit.MB).getMbFrac(),
            0.001
        );

        assertEquals(
            ((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy(false)).getMaxMergeAtOnce(),
            TieredMergePolicyProvider.DEFAULT_MAX_MERGE_AT_ONCE
        );
        indexSettings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(
                        TieredMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING.getKey(),
                        TieredMergePolicyProvider.DEFAULT_MAX_MERGE_AT_ONCE - 1
                    )
                    .build()
            )
        );
        assertEquals(
            ((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy(false)).getMaxMergeAtOnce(),
            TieredMergePolicyProvider.DEFAULT_MAX_MERGE_AT_ONCE - 1
        );

        assertEquals(
            ((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy(false)).getMaxMergedSegmentMB(),
            TieredMergePolicyProvider.DEFAULT_MAX_MERGED_SEGMENT.getMbFrac(),
            0.0001
        );
        indexSettings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(
                        TieredMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGED_SEGMENT_SETTING.getKey(),
                        new ByteSizeValue(TieredMergePolicyProvider.DEFAULT_MAX_MERGED_SEGMENT.getBytes() + 1)
                    )
                    .build()
            )
        );
        assertEquals(
            ((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy(false)).getMaxMergedSegmentMB(),
            new ByteSizeValue(TieredMergePolicyProvider.DEFAULT_MAX_MERGED_SEGMENT.getBytes() + 1).getMbFrac(),
            0.0001
        );

        assertEquals(
            ((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy(false)).getSegmentsPerTier(),
            TieredMergePolicyProvider.DEFAULT_SEGMENTS_PER_TIER,
            0
        );
        indexSettings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(
                        TieredMergePolicyProvider.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING.getKey(),
                        TieredMergePolicyProvider.DEFAULT_SEGMENTS_PER_TIER + 1
                    )
                    .build()
            )
        );
        assertEquals(
            ((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy(false)).getSegmentsPerTier(),
            TieredMergePolicyProvider.DEFAULT_SEGMENTS_PER_TIER + 1,
            0
        );

        assertEquals(
            ((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy(false)).getDeletesPctAllowed(),
            TieredMergePolicyProvider.DEFAULT_DELETES_PCT_ALLOWED,
            0
        );
        indexSettings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder().put(TieredMergePolicyProvider.INDEX_MERGE_POLICY_DELETES_PCT_ALLOWED_SETTING.getKey(), 22).build()
            )
        );
        assertEquals(((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy(false)).getDeletesPctAllowed(), 22, 0);

        IllegalArgumentException exc = expectThrows(
            IllegalArgumentException.class,
            () -> indexSettings.updateIndexMetadata(
                newIndexMeta(
                    "index",
                    Settings.builder().put(TieredMergePolicyProvider.INDEX_MERGE_POLICY_DELETES_PCT_ALLOWED_SETTING.getKey(), 53).build()
                )
            )
        );
        final Throwable cause = exc.getCause();
        assertThat(cause.getMessage(), containsString("must be <= 50.0"));
        indexSettings.updateIndexMetadata(newIndexMeta("index", EMPTY_SETTINGS)); // see if defaults are restored
        assertEquals(
            ((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy(false)).getForceMergeDeletesPctAllowed(),
            TieredMergePolicyProvider.DEFAULT_EXPUNGE_DELETES_ALLOWED,
            0.0d
        );
        assertEquals(
            ((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy(false)).getFloorSegmentMB(),
            new ByteSizeValue(TieredMergePolicyProvider.DEFAULT_FLOOR_SEGMENT.getMb(), ByteSizeUnit.MB).getMbFrac(),
            0.00
        );
        assertEquals(
            ((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy(false)).getMaxMergeAtOnce(),
            TieredMergePolicyProvider.DEFAULT_MAX_MERGE_AT_ONCE
        );
        assertEquals(
            ((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy(false)).getMaxMergedSegmentMB(),
            new ByteSizeValue(TieredMergePolicyProvider.DEFAULT_MAX_MERGED_SEGMENT.getBytes() + 1).getMbFrac(),
            0.0001
        );
        assertEquals(
            ((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy(false)).getSegmentsPerTier(),
            TieredMergePolicyProvider.DEFAULT_SEGMENTS_PER_TIER,
            0
        );
        assertEquals(
            ((OpenSearchTieredMergePolicy) indexSettings.getMergePolicy(false)).getDeletesPctAllowed(),
            TieredMergePolicyProvider.DEFAULT_DELETES_PCT_ALLOWED,
            0
        );
    }

    public void testLogByteSizeMergePolicySettingsUpdate() throws IOException {

        IndexSettings indexSettings = indexSettings(
            Settings.builder().put(IndexSettings.INDEX_MERGE_POLICY.getKey(), IndexSettings.LOG_BYTE_SIZE_MERGE_POLICY).build()
        );
        assertEquals(
            ((LogByteSizeMergePolicy) indexSettings.getMergePolicy(true)).getMergeFactor(),
            TieredMergePolicyProvider.DEFAULT_MAX_MERGE_AT_ONCE
        );

        indexSettings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(IndexSettings.INDEX_MERGE_POLICY.getKey(), "log_byte_size")
                    .put(
                        LogByteSizeMergePolicyProvider.INDEX_LBS_MERGE_POLICY_MERGE_FACTOR_SETTING.getKey(),
                        LogByteSizeMergePolicyProvider.DEFAULT_MERGE_FACTOR + 1
                    )
                    .build()
            )
        );
        assertEquals(
            ((LogByteSizeMergePolicy) indexSettings.getMergePolicy(true)).getMergeFactor(),
            TieredMergePolicyProvider.DEFAULT_MAX_MERGE_AT_ONCE + 1
        );

        indexSettings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(IndexSettings.INDEX_MERGE_POLICY.getKey(), "log_byte_size")
                    .put(
                        LogByteSizeMergePolicyProvider.INDEX_LBS_MERGE_POLICY_MIN_MERGE_MB_SETTING.getKey(),
                        new ByteSizeValue(LogByteSizeMergePolicyProvider.DEFAULT_MIN_MERGE_MB.getMb() + 1, ByteSizeUnit.MB)
                    )
                    .build()
            )
        );

        assertEquals(
            ((LogByteSizeMergePolicy) indexSettings.getMergePolicy(true)).getMinMergeMB(),
            new ByteSizeValue(TieredMergePolicyProvider.DEFAULT_FLOOR_SEGMENT.getMb() + 1, ByteSizeUnit.MB).getMbFrac(),
            0.001
        );

        indexSettings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(IndexSettings.INDEX_MERGE_POLICY.getKey(), "log_byte_size")
                    .put(
                        LogByteSizeMergePolicyProvider.INDEX_LBS_MAX_MERGE_SEGMENT_MB_SETTING.getKey(),
                        new ByteSizeValue(LogByteSizeMergePolicyProvider.DEFAULT_MAX_MERGED_SEGMENT.getMb() + 100, ByteSizeUnit.MB)
                    )
                    .build()
            )
        );

        assertEquals(
            ((LogByteSizeMergePolicy) indexSettings.getMergePolicy(true)).getMaxMergeMB(),
            new ByteSizeValue(TieredMergePolicyProvider.DEFAULT_MAX_MERGED_SEGMENT.getMb() + 100, ByteSizeUnit.MB).getMbFrac(),
            0.001
        );

        indexSettings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(IndexSettings.INDEX_MERGE_POLICY.getKey(), "log_byte_size")
                    .put(
                        LogByteSizeMergePolicyProvider.INDEX_LBS_MAX_MERGE_SEGMENT_MB_FOR_FORCED_MERGE_SETTING.getKey(),
                        new ByteSizeValue(
                            LogByteSizeMergePolicyProvider.DEFAULT_MAX_MERGE_SEGMENT_MB_FORCE_MERGE.getMb() + 1,
                            ByteSizeUnit.MB
                        )
                    )
                    .build()
            )
        );
        assertEquals(
            ((LogByteSizeMergePolicy) indexSettings.getMergePolicy(true)).getMaxMergeMBForForcedMerge(),
            new ByteSizeValue(LogByteSizeMergePolicyProvider.DEFAULT_MAX_MERGE_SEGMENT_MB_FORCE_MERGE.getMb() + 1, ByteSizeUnit.MB)
                .getMbFrac(),
            0.001
        );

        indexSettings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(IndexSettings.INDEX_MERGE_POLICY.getKey(), "log_byte_size")
                    .put(LogByteSizeMergePolicyProvider.INDEX_LBS_MAX_MERGED_DOCS_SETTING.getKey(), 10000000)
                    .build()
            )
        );
        assertEquals(((LogByteSizeMergePolicy) indexSettings.getMergePolicy(true)).getMaxMergeDocs(), 10000000);

        indexSettings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(IndexSettings.INDEX_MERGE_POLICY.getKey(), "log_byte_size")
                    .put(LogByteSizeMergePolicyProvider.INDEX_LBS_NO_CFS_RATIO_SETTING.getKey(), 0.1)
                    .build()
            )
        );
        assertEquals(indexSettings.getMergePolicy(true).getNoCFSRatio(), 0.1, 0.0);
    }

    public Settings build(String value) {
        return Settings.builder().put(TieredMergePolicyProvider.INDEX_COMPOUND_FORMAT_SETTING.getKey(), value).build();
    }

    public Settings build(double value) {
        return Settings.builder().put(TieredMergePolicyProvider.INDEX_COMPOUND_FORMAT_SETTING.getKey(), value).build();
    }

    public Settings build(int value) {
        return Settings.builder().put(TieredMergePolicyProvider.INDEX_COMPOUND_FORMAT_SETTING.getKey(), value).build();
    }

    public Settings build(boolean value) {
        return Settings.builder().put(TieredMergePolicyProvider.INDEX_COMPOUND_FORMAT_SETTING.getKey(), value).build();
    }

}
