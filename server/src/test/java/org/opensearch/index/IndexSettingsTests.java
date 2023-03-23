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

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.AbstractScopedSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.FeatureFlagSetter;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.StringContains.containsString;
import static org.hamcrest.object.HasToString.hasToString;
import static org.opensearch.index.store.remote.directory.RemoteSnapshotDirectory.SEARCHABLE_SNAPSHOT_EXTENDED_COMPATIBILITY_MINIMUM_VERSION;

public class IndexSettingsTests extends OpenSearchTestCase {

    public void testRunListener() {
        Version version = VersionUtils.getPreviousVersion();
        Settings theSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, version)
            .put(IndexMetadata.SETTING_INDEX_UUID, "0xdeadbeef")
            .build();
        final AtomicInteger integer = new AtomicInteger(0);
        Setting<Integer> integerSetting = Setting.intSetting("index.test.setting.int", -1, Property.Dynamic, Property.IndexScope);
        IndexMetadata metadata = newIndexMeta("index", theSettings);
        IndexSettings settings = newIndexSettings(newIndexMeta("index", theSettings), Settings.EMPTY, integerSetting);
        settings.getScopedSettings().addSettingsUpdateConsumer(integerSetting, integer::set);

        assertEquals(version, settings.getIndexVersionCreated());
        assertEquals("0xdeadbeef", settings.getUUID());

        assertFalse(settings.updateIndexMetadata(metadata));
        assertEquals(metadata.getSettings(), settings.getSettings());
        assertEquals(0, integer.get());
        assertTrue(
            settings.updateIndexMetadata(
                newIndexMeta("index", Settings.builder().put(theSettings).put("index.test.setting.int", 42).build())
            )
        );
        assertEquals(42, integer.get());
    }

    public void testSettingsUpdateValidator() {
        Version version = VersionUtils.getPreviousVersion();
        Settings theSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, version)
            .put(IndexMetadata.SETTING_INDEX_UUID, "0xdeadbeef")
            .build();
        final AtomicInteger integer = new AtomicInteger(0);
        Setting<Integer> integerSetting = Setting.intSetting("index.test.setting.int", -1, Property.Dynamic, Property.IndexScope);
        IndexMetadata metadata = newIndexMeta("index", theSettings);
        IndexSettings settings = newIndexSettings(newIndexMeta("index", theSettings), Settings.EMPTY, integerSetting);
        settings.getScopedSettings().addSettingsUpdateConsumer(integerSetting, integer::set, (i) -> {
            if (i == 42) throw new AssertionError("boom");
        });

        assertEquals(version, settings.getIndexVersionCreated());
        assertEquals("0xdeadbeef", settings.getUUID());

        assertFalse(settings.updateIndexMetadata(metadata));
        assertEquals(metadata.getSettings(), settings.getSettings());
        assertEquals(0, integer.get());
        expectThrows(
            IllegalArgumentException.class,
            () -> settings.updateIndexMetadata(
                newIndexMeta("index", Settings.builder().put(theSettings).put("index.test.setting.int", 42).build())
            )
        );
        assertTrue(
            settings.updateIndexMetadata(
                newIndexMeta("index", Settings.builder().put(theSettings).put("index.test.setting.int", 41).build())
            )
        );
        assertEquals(41, integer.get());
    }

    public void testMergedSettingsArePassed() {
        Version version = VersionUtils.getPreviousVersion();
        Settings theSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, version)
            .put(IndexMetadata.SETTING_INDEX_UUID, "0xdeadbeef")
            .build();
        final AtomicInteger integer = new AtomicInteger(0);
        final StringBuilder builder = new StringBuilder();
        Setting<Integer> integerSetting = Setting.intSetting("index.test.setting.int", -1, Property.Dynamic, Property.IndexScope);
        Setting<String> notUpdated = new Setting<>("index.not.updated", "", Function.identity(), Property.Dynamic, Property.IndexScope);

        IndexSettings settings = newIndexSettings(newIndexMeta("index", theSettings), Settings.EMPTY, integerSetting, notUpdated);
        settings.getScopedSettings().addSettingsUpdateConsumer(integerSetting, integer::set);
        settings.getScopedSettings().addSettingsUpdateConsumer(notUpdated, builder::append);
        assertEquals(0, integer.get());
        assertEquals("", builder.toString());
        IndexMetadata newMetadata = newIndexMeta(
            "index",
            Settings.builder().put(settings.getIndexMetadata().getSettings()).put("index.test.setting.int", 42).build()
        );
        assertTrue(settings.updateIndexMetadata(newMetadata));
        assertSame(settings.getIndexMetadata(), newMetadata);
        assertEquals(42, integer.get());
        assertEquals("", builder.toString());
        integer.set(0);
        assertTrue(
            settings.updateIndexMetadata(
                newIndexMeta(
                    "index",
                    Settings.builder().put(settings.getIndexMetadata().getSettings()).put("index.not.updated", "boom").build()
                )
            )
        );
        assertEquals("boom", builder.toString());
        assertEquals("not updated - we preserve the old settings", 0, integer.get());

    }

    public void testSettingsConsistency() {
        Version version = VersionUtils.getPreviousVersion();
        IndexMetadata metadata = newIndexMeta("index", Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build());
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(version, settings.getIndexVersionCreated());
        assertEquals("_na_", settings.getUUID());
        try {
            settings.updateIndexMetadata(
                newIndexMeta(
                    "index",
                    Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).put("index.test.setting.int", 42).build()
                )
            );
            fail("version has changed");
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().startsWith("version mismatch on settings update expected: "));
        }

        // use version number that is unknown
        metadata = newIndexMeta("index", Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.fromId(999999)).build());
        settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(Version.fromId(999999), settings.getIndexVersionCreated());
        assertEquals("_na_", settings.getUUID());
        settings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.fromId(999999))
                    .put("index.test.setting.int", 42)
                    .build()
            )
        );

        metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, "0xdeadbeef")
                .build()
        );
        settings = new IndexSettings(metadata, Settings.EMPTY);
        try {
            settings.updateIndexMetadata(
                newIndexMeta(
                    "index",
                    Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).put("index.test.setting.int", 42).build()
                )
            );
            fail("uuid missing/change");
        } catch (IllegalArgumentException ex) {
            assertEquals("uuid mismatch on settings update expected: 0xdeadbeef but was: _na_", ex.getMessage());
        }
        assertEquals(metadata.getSettings(), settings.getSettings());
    }

    public IndexSettings newIndexSettings(IndexMetadata metadata, Settings nodeSettings, Setting<?>... settings) {
        Set<Setting<?>> settingSet = new HashSet<>(IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);
        if (settings.length > 0) {
            settingSet.addAll(Arrays.asList(settings));
        }
        return new IndexSettings(metadata, nodeSettings, new IndexScopedSettings(Settings.EMPTY, settingSet));
    }

    public void testNodeSettingsAreContained() {
        final int numShards = randomIntBetween(1, 10);
        final int numReplicas = randomIntBetween(0, 10);
        Settings theSettings = Settings.builder()
            .put("index.foo.bar", 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
            .build();

        Settings nodeSettings = Settings.builder().put("index.foo.bar", 43).build();
        final AtomicInteger indexValue = new AtomicInteger(0);
        Setting<Integer> integerSetting = Setting.intSetting("index.foo.bar", -1, Property.Dynamic, Property.IndexScope);
        IndexSettings settings = newIndexSettings(newIndexMeta("index", theSettings), nodeSettings, integerSetting);
        settings.getScopedSettings().addSettingsUpdateConsumer(integerSetting, indexValue::set);
        assertEquals(numReplicas, settings.getNumberOfReplicas());
        assertEquals(numShards, settings.getNumberOfShards());
        assertEquals(0, indexValue.get());

        assertTrue(
            settings.updateIndexMetadata(
                newIndexMeta(
                    "index",
                    Settings.builder()
                        .put("index.foo.bar", 42)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas + 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                        .build()
                )
            )
        );

        assertEquals(42, indexValue.get());
        assertSame(nodeSettings, settings.getNodeSettings());

        assertTrue(
            settings.updateIndexMetadata(
                newIndexMeta(
                    "index",
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas + 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                        .build()
                )
            )
        );
        assertEquals(43, indexValue.get());

    }

    public static IndexMetadata newIndexMeta(String name, Settings indexSettings) {
        Settings build = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(indexSettings)
            .build();
        return IndexMetadata.builder(name).settings(build).build();
    }

    public void testUpdateDurability() {
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), "async")
                .build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(Translog.Durability.ASYNC, settings.getTranslogDurability());
        settings.updateIndexMetadata(
            newIndexMeta("index", Settings.builder().put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), "request").build())
        );
        assertEquals(Translog.Durability.REQUEST, settings.getTranslogDurability());

        metadata = newIndexMeta("index", Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build());
        settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(Translog.Durability.REQUEST, settings.getTranslogDurability()); // test default
    }

    public void testIsWarmerEnabled() {
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexSettings.INDEX_WARMER_ENABLED_SETTING.getKey(), false)
                .build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertFalse(settings.isWarmerEnabled());
        settings.updateIndexMetadata(
            newIndexMeta("index", Settings.builder().put(IndexSettings.INDEX_WARMER_ENABLED_SETTING.getKey(), "true").build())
        );
        assertTrue(settings.isWarmerEnabled());
        metadata = newIndexMeta("index", Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build());
        settings = new IndexSettings(metadata, Settings.EMPTY);
        assertTrue(settings.isWarmerEnabled());
    }

    public void testRefreshInterval() {
        String refreshInterval = getRandomTimeString();
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), refreshInterval)
                .build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(
            TimeValue.parseTimeValue(
                refreshInterval,
                new TimeValue(1, TimeUnit.DAYS),
                IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey()
            ),
            settings.getRefreshInterval()
        );
        String newRefreshInterval = getRandomTimeString();
        settings.updateIndexMetadata(
            newIndexMeta("index", Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), newRefreshInterval).build())
        );
        assertEquals(
            TimeValue.parseTimeValue(
                newRefreshInterval,
                new TimeValue(1, TimeUnit.DAYS),
                IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey()
            ),
            settings.getRefreshInterval()
        );
    }

    private String getRandomTimeString() {
        int refreshIntervalInt = randomFrom(-1, Math.abs(randomInt()));
        String refreshInterval = Integer.toString(refreshIntervalInt);
        if (refreshIntervalInt >= 0) {
            refreshInterval += randomFrom("s", "ms", "h");
        }
        return refreshInterval;
    }

    public void testMaxResultWindow() {
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey(), 15)
                .build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(15, settings.getMaxResultWindow());
        settings.updateIndexMetadata(
            newIndexMeta("index", Settings.builder().put(IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey(), 42).build())
        );
        assertEquals(42, settings.getMaxResultWindow());
        settings.updateIndexMetadata(newIndexMeta("index", Settings.EMPTY));
        assertEquals(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY).intValue(), settings.getMaxResultWindow());

        metadata = newIndexMeta("index", Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build());
        settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(IndexSettings.MAX_RESULT_WINDOW_SETTING.get(Settings.EMPTY).intValue(), settings.getMaxResultWindow());
    }

    public void testMaxInnerResultWindow() {
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexSettings.MAX_INNER_RESULT_WINDOW_SETTING.getKey(), 200)
                .build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(200, settings.getMaxInnerResultWindow());
        settings.updateIndexMetadata(
            newIndexMeta("index", Settings.builder().put(IndexSettings.MAX_INNER_RESULT_WINDOW_SETTING.getKey(), 50).build())
        );
        assertEquals(50, settings.getMaxInnerResultWindow());
        settings.updateIndexMetadata(newIndexMeta("index", Settings.EMPTY));
        assertEquals(IndexSettings.MAX_INNER_RESULT_WINDOW_SETTING.get(Settings.EMPTY).intValue(), settings.getMaxInnerResultWindow());

        metadata = newIndexMeta("index", Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build());
        settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(IndexSettings.MAX_INNER_RESULT_WINDOW_SETTING.get(Settings.EMPTY).intValue(), settings.getMaxInnerResultWindow());
    }

    public void testMaxDocvalueFields() {
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexSettings.MAX_DOCVALUE_FIELDS_SEARCH_SETTING.getKey(), 200)
                .build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(200, settings.getMaxDocvalueFields());
        settings.updateIndexMetadata(
            newIndexMeta("index", Settings.builder().put(IndexSettings.MAX_DOCVALUE_FIELDS_SEARCH_SETTING.getKey(), 50).build())
        );
        assertEquals(50, settings.getMaxDocvalueFields());
        settings.updateIndexMetadata(newIndexMeta("index", Settings.EMPTY));
        assertEquals(IndexSettings.MAX_DOCVALUE_FIELDS_SEARCH_SETTING.get(Settings.EMPTY).intValue(), settings.getMaxDocvalueFields());

        metadata = newIndexMeta("index", Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build());
        settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(IndexSettings.MAX_DOCVALUE_FIELDS_SEARCH_SETTING.get(Settings.EMPTY).intValue(), settings.getMaxDocvalueFields());
    }

    public void testMaxScriptFields() {
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexSettings.MAX_SCRIPT_FIELDS_SETTING.getKey(), 100)
                .build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(100, settings.getMaxScriptFields());
        settings.updateIndexMetadata(
            newIndexMeta("index", Settings.builder().put(IndexSettings.MAX_SCRIPT_FIELDS_SETTING.getKey(), 20).build())
        );
        assertEquals(20, settings.getMaxScriptFields());
        settings.updateIndexMetadata(newIndexMeta("index", Settings.EMPTY));
        assertEquals(IndexSettings.MAX_SCRIPT_FIELDS_SETTING.get(Settings.EMPTY).intValue(), settings.getMaxScriptFields());

        metadata = newIndexMeta("index", Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build());
        settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(IndexSettings.MAX_SCRIPT_FIELDS_SETTING.get(Settings.EMPTY).intValue(), settings.getMaxScriptFields());
    }

    public void testMaxAdjacencyMatrixFiltersSetting() {
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexSettings.MAX_ADJACENCY_MATRIX_FILTERS_SETTING.getKey(), 15)
                .build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(15, settings.getMaxAdjacencyMatrixFilters());
        settings.updateIndexMetadata(
            newIndexMeta("index", Settings.builder().put(IndexSettings.MAX_ADJACENCY_MATRIX_FILTERS_SETTING.getKey(), 42).build())
        );
        assertEquals(42, settings.getMaxAdjacencyMatrixFilters());
        settings.updateIndexMetadata(newIndexMeta("index", Settings.EMPTY));
        assertEquals(
            IndexSettings.MAX_ADJACENCY_MATRIX_FILTERS_SETTING.get(Settings.EMPTY).intValue(),
            settings.getMaxAdjacencyMatrixFilters()
        );

        metadata = newIndexMeta("index", Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build());
        settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(
            IndexSettings.MAX_ADJACENCY_MATRIX_FILTERS_SETTING.get(Settings.EMPTY).intValue(),
            settings.getMaxAdjacencyMatrixFilters()
        );
        assertWarnings(
            "[index.max_adjacency_matrix_filters] setting was deprecated in OpenSearch and will be removed in a "
                + "future release! See the breaking changes documentation for the next major version."
        );
    }

    public void testMaxRegexLengthSetting() {
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexSettings.MAX_REGEX_LENGTH_SETTING.getKey(), 99)
                .build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(99, settings.getMaxRegexLength());
        settings.updateIndexMetadata(
            newIndexMeta("index", Settings.builder().put(IndexSettings.MAX_REGEX_LENGTH_SETTING.getKey(), 101).build())
        );
        assertEquals(101, settings.getMaxRegexLength());
        settings.updateIndexMetadata(newIndexMeta("index", Settings.EMPTY));
        assertEquals(IndexSettings.MAX_REGEX_LENGTH_SETTING.get(Settings.EMPTY).intValue(), settings.getMaxRegexLength());

        metadata = newIndexMeta("index", Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build());
        settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(IndexSettings.MAX_REGEX_LENGTH_SETTING.get(Settings.EMPTY).intValue(), settings.getMaxRegexLength());
    }

    public void testGCDeletesSetting() {
        TimeValue gcDeleteSetting = new TimeValue(Math.abs(randomInt()), TimeUnit.MILLISECONDS);
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexSettings.INDEX_GC_DELETES_SETTING.getKey(), gcDeleteSetting.getStringRep())
                .build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(
            TimeValue.parseTimeValue(
                gcDeleteSetting.getStringRep(),
                new TimeValue(1, TimeUnit.DAYS),
                IndexSettings.INDEX_GC_DELETES_SETTING.getKey()
            ).getMillis(),
            settings.getGcDeletesInMillis()
        );
        TimeValue newGCDeleteSetting = new TimeValue(Math.abs(randomInt()), TimeUnit.MILLISECONDS);
        settings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder().put(IndexSettings.INDEX_GC_DELETES_SETTING.getKey(), newGCDeleteSetting.getStringRep()).build()
            )
        );
        assertEquals(
            TimeValue.parseTimeValue(
                newGCDeleteSetting.getStringRep(),
                new TimeValue(1, TimeUnit.DAYS),
                IndexSettings.INDEX_GC_DELETES_SETTING.getKey()
            ).getMillis(),
            settings.getGcDeletesInMillis()
        );
        settings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(
                        IndexSettings.INDEX_GC_DELETES_SETTING.getKey(),
                        (randomBoolean() ? -1 : new TimeValue(-1, TimeUnit.MILLISECONDS)).toString()
                    )
                    .build()
            )
        );
        assertEquals(-1, settings.getGcDeletesInMillis());
    }

    public void testTranslogFlushSizeThreshold() {
        ByteSizeValue translogFlushThresholdSize = new ByteSizeValue(Math.abs(randomInt()));
        ByteSizeValue actualValue = ByteSizeValue.parseBytesSizeValue(
            translogFlushThresholdSize.getBytes() + "B",
            IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey()
        );
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), translogFlushThresholdSize.getBytes() + "B")
                .build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(actualValue, settings.getFlushThresholdSize());
        ByteSizeValue newTranslogFlushThresholdSize = new ByteSizeValue(Math.abs(randomInt()));
        ByteSizeValue actualNewTranslogFlushThresholdSize = ByteSizeValue.parseBytesSizeValue(
            newTranslogFlushThresholdSize.getBytes() + "B",
            IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey()
        );
        settings.updateIndexMetadata(
            newIndexMeta(
                "index",
                Settings.builder()
                    .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), newTranslogFlushThresholdSize.getBytes() + "B")
                    .build()
            )
        );
        assertEquals(actualNewTranslogFlushThresholdSize, settings.getFlushThresholdSize());
    }

    public void testTranslogGenerationSizeThreshold() {
        final ByteSizeValue size = new ByteSizeValue(Math.abs(randomInt()));
        final String key = IndexSettings.INDEX_TRANSLOG_GENERATION_THRESHOLD_SIZE_SETTING.getKey();
        final ByteSizeValue actualValue = ByteSizeValue.parseBytesSizeValue(size.getBytes() + "B", key);
        final IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).put(key, size.getBytes() + "B").build()
        );
        final IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals(actualValue, settings.getGenerationThresholdSize());
        final ByteSizeValue newSize = new ByteSizeValue(Math.abs(randomInt()));
        final ByteSizeValue actual = ByteSizeValue.parseBytesSizeValue(newSize.getBytes() + "B", key);
        settings.updateIndexMetadata(newIndexMeta("index", Settings.builder().put(key, newSize.getBytes() + "B").build()));
        assertEquals(actual, settings.getGenerationThresholdSize());
    }

    public void testPrivateSettingsValidation() {
        final Settings settings = Settings.builder().put(IndexMetadata.SETTING_CREATION_DATE, System.currentTimeMillis()).build();
        final IndexScopedSettings indexScopedSettings = new IndexScopedSettings(settings, IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);

        {
            // validation should fail since we are not ignoring private settings
            final SettingsException e = expectThrows(
                SettingsException.class,
                () -> indexScopedSettings.validate(settings, randomBoolean())
            );
            assertThat(e, hasToString(containsString("unknown setting [index.creation_date]")));
        }

        {
            // validation should fail since we are not ignoring private settings
            final SettingsException e = expectThrows(
                SettingsException.class,
                () -> indexScopedSettings.validate(settings, randomBoolean(), false, randomBoolean())
            );
            assertThat(e, hasToString(containsString("unknown setting [index.creation_date]")));
        }

        // nothing should happen since we are ignoring private settings
        indexScopedSettings.validate(settings, randomBoolean(), true, randomBoolean());
    }

    public void testArchivedSettingsValidation() {
        final Settings settings = Settings.builder()
            .put(AbstractScopedSettings.ARCHIVED_SETTINGS_PREFIX + "foo", System.currentTimeMillis())
            .build();
        final IndexScopedSettings indexScopedSettings = new IndexScopedSettings(settings, IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);

        {
            // validation should fail since we are not ignoring archived settings
            final SettingsException e = expectThrows(
                SettingsException.class,
                () -> indexScopedSettings.validate(settings, randomBoolean())
            );
            assertThat(e, hasToString(containsString("unknown setting [archived.foo]")));
        }

        {
            // validation should fail since we are not ignoring archived settings
            final SettingsException e = expectThrows(
                SettingsException.class,
                () -> indexScopedSettings.validate(settings, randomBoolean(), randomBoolean(), false)
            );
            assertThat(e, hasToString(containsString("unknown setting [archived.foo]")));
        }

        // nothing should happen since we are ignoring archived settings
        indexScopedSettings.validate(settings, randomBoolean(), randomBoolean(), true);
    }

    public void testArchiveBrokenIndexSettings() {
        Settings settings = IndexScopedSettings.DEFAULT_SCOPED_SETTINGS.archiveUnknownOrInvalidSettings(Settings.EMPTY, e -> {
            assert false : "should not have been invoked, no unknown settings";
        }, (e, ex) -> { assert false : "should not have been invoked, no invalid settings"; });
        assertSame(settings, Settings.EMPTY);
        settings = IndexScopedSettings.DEFAULT_SCOPED_SETTINGS.archiveUnknownOrInvalidSettings(
            Settings.builder().put("index.refresh_interval", "-200").build(),
            e -> {
                assert false : "should not have been invoked, no invalid settings";
            },
            (e, ex) -> {
                assertThat(e.getKey(), equalTo("index.refresh_interval"));
                assertThat(e.getValue(), equalTo("-200"));
                assertThat(ex, hasToString(containsString("failed to parse setting [index.refresh_interval] with value [-200]")));
            }
        );
        assertEquals("-200", settings.get("archived.index.refresh_interval"));
        assertNull(settings.get("index.refresh_interval"));

        Settings prevSettings = settings; // no double archive
        settings = IndexScopedSettings.DEFAULT_SCOPED_SETTINGS.archiveUnknownOrInvalidSettings(prevSettings, e -> {
            assert false : "should not have been invoked, no unknown settings";
        }, (e, ex) -> { assert false : "should not have been invoked, no invalid settings"; });
        assertSame(prevSettings, settings);

        settings = IndexScopedSettings.DEFAULT_SCOPED_SETTINGS.archiveUnknownOrInvalidSettings(
            Settings.builder()
                .put("index.version.created", Version.CURRENT.id) // private setting
                .put("index.unknown", "foo")
                .put("index.refresh_interval", "2s")
                .build(),
            e -> {
                assertThat(e.getKey(), equalTo("index.unknown"));
                assertThat(e.getValue(), equalTo("foo"));
            },
            (e, ex) -> { assert false : "should not have been invoked, no invalid settings"; }
        );

        assertEquals("foo", settings.get("archived.index.unknown"));
        assertEquals(Integer.toString(Version.CURRENT.id), settings.get("index.version.created"));
        assertEquals("2s", settings.get("index.refresh_interval"));
    }

    public void testQueryDefaultField() {
        IndexSettings index = newIndexSettings(newIndexMeta("index", Settings.EMPTY), Settings.EMPTY);
        assertThat(index.getDefaultFields(), equalTo(Collections.singletonList("*")));
        index = newIndexSettings(
            newIndexMeta("index", Settings.EMPTY),
            Settings.builder().put("index.query.default_field", "body").build()
        );
        assertThat(index.getDefaultFields(), equalTo(Collections.singletonList("body")));
        index.updateIndexMetadata(newIndexMeta("index", Settings.builder().putList("index.query.default_field", "body", "title").build()));
        assertThat(index.getDefaultFields(), equalTo(Arrays.asList("body", "title")));
    }

    public void testUpdateSoftDeletesFails() {
        IndexScopedSettings settings = new IndexScopedSettings(Settings.EMPTY, IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);
        SettingsException error = expectThrows(
            SettingsException.class,
            () -> settings.updateSettings(
                Settings.builder().put("index.soft_deletes.enabled", randomBoolean()).build(),
                Settings.builder(),
                Settings.builder(),
                "index"
            )
        );
        assertThat(error.getMessage(), equalTo("final index setting [index.soft_deletes.enabled], not updateable"));
    }

    public void testSoftDeletesDefaultSetting() {
        // enabled by default on 7.0+ or later
        {
            Version createdVersion = VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.CURRENT);
            Settings settings = Settings.builder().put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), createdVersion).build();
            assertTrue(IndexSettings.INDEX_SOFT_DELETES_SETTING.get(settings));
        }
    }

    public void testIgnoreTranslogRetentionSettingsIfSoftDeletesEnabled() {
        Settings.Builder settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.CURRENT));
        if (randomBoolean()) {
            settings.put(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey(), randomPositiveTimeValue());
        }
        if (randomBoolean()) {
            settings.put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), between(1, 1024) + "b");
        }
        IndexMetadata metadata = newIndexMeta("index", settings.build());
        IndexSettings indexSettings = new IndexSettings(metadata, Settings.EMPTY);
        assertThat(indexSettings.getTranslogRetentionAge().millis(), equalTo(-1L));
        assertThat(indexSettings.getTranslogRetentionSize().getBytes(), equalTo(-1L));

        Settings.Builder newSettings = Settings.builder().put(settings.build());
        if (randomBoolean()) {
            newSettings.put(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey(), randomPositiveTimeValue());
        }
        if (randomBoolean()) {
            newSettings.put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), between(1, 1024) + "b");
        }
        indexSettings.updateIndexMetadata(newIndexMeta("index", newSettings.build()));
        assertThat(indexSettings.getTranslogRetentionAge().millis(), equalTo(-1L));
        assertThat(indexSettings.getTranslogRetentionSize().getBytes(), equalTo(-1L));
    }

    public void testRemoteStoreDefaultSetting() {
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertFalse(settings.isRemoteStoreEnabled());
    }

    public void testRemoteStoreExplicitSetting() {
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
                .build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertTrue(settings.isRemoteStoreEnabled());
    }

    public void testRemoteTranslogStoreDefaultSetting() {
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertFalse(settings.isRemoteTranslogStoreEnabled());
    }

    public void testRemoteTranslogStoreExplicitSetting() {
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_ENABLED, true)
                .build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertTrue(settings.isRemoteTranslogStoreEnabled());
    }

    public void testRemoteTranslogStoreNullSetting() {
        Settings indexSettings = Settings.builder()
            .put("index.remote_store.translog.enabled", "null")
            .put("index.remote_store.enabled", randomBoolean())
            .build();
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> IndexMetadata.INDEX_REMOTE_TRANSLOG_STORE_ENABLED_SETTING.get(indexSettings)
        );
        assertEquals("Failed to parse value [null] as only [true] or [false] are allowed.", iae.getMessage());
    }

    public void testUpdateRemoteStoreFails() {
        Set<Setting<?>> remoteStoreSettingSet = new HashSet<>();
        remoteStoreSettingSet.add(IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING);
        IndexScopedSettings settings = new IndexScopedSettings(Settings.EMPTY, remoteStoreSettingSet);
        SettingsException error = expectThrows(
            SettingsException.class,
            () -> settings.updateSettings(
                Settings.builder().put("index.remote_store.enabled", randomBoolean()).build(),
                Settings.builder(),
                Settings.builder(),
                "index"
            )
        );
        assertEquals(error.getMessage(), "final index setting [index.remote_store.enabled], not updateable");
    }

    public void testUpdateRemoteTranslogStoreFails() {
        Set<Setting<?>> remoteStoreSettingSet = new HashSet<>();
        remoteStoreSettingSet.add(IndexMetadata.INDEX_REMOTE_TRANSLOG_STORE_ENABLED_SETTING);
        IndexScopedSettings settings = new IndexScopedSettings(Settings.EMPTY, remoteStoreSettingSet);
        SettingsException error = expectThrows(
            SettingsException.class,
            () -> settings.updateSettings(
                Settings.builder().put("index.remote_store.translog.enabled", randomBoolean()).build(),
                Settings.builder(),
                Settings.builder(),
                "index"
            )
        );
        assertEquals(error.getMessage(), "final index setting [index.remote_store.translog.enabled], not updateable");
    }

    public void testEnablingRemoteTranslogStoreFailsWhenRemoteSegmentDisabled() {
        Settings indexSettings = Settings.builder()
            .put("index.remote_store.translog.enabled", true)
            .put("index.remote_store.enabled", false)
            .build();
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> IndexMetadata.INDEX_REMOTE_TRANSLOG_STORE_ENABLED_SETTING.get(indexSettings)
        );
        assertEquals(
            "Settings index.remote_store.translog.enabled can ont be set/enabled when index.remote_store.enabled is set to true",
            iae.getMessage()
        );
    }

    public void testEnablingRemoteStoreFailsWhenReplicationTypeIsDocument() {
        Settings indexSettings = Settings.builder()
            .put("index.replication.type", ReplicationType.DOCUMENT)
            .put("index.remote_store.enabled", true)
            .build();
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.get(indexSettings)
        );
        assertEquals("To enable index.remote_store.enabled, index.replication.type should be set to SEGMENT", iae.getMessage());
    }

    public void testEnablingRemoteStoreFailsWhenReplicationTypeIsDefault() {
        Settings indexSettings = Settings.builder().put("index.remote_store.enabled", true).build();
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.get(indexSettings)
        );
        assertEquals("To enable index.remote_store.enabled, index.replication.type should be set to SEGMENT", iae.getMessage());
    }

    public void testRemoteRepositoryDefaultSetting() {
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertNull(settings.getRemoteStoreRepository());
    }

    public void testRemoteRepositoryExplicitSetting() {
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
                .put(IndexMetadata.SETTING_REMOTE_STORE_REPOSITORY, "repo1")
                .build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertEquals("repo1", settings.getRemoteStoreRepository());
    }

    public void testUpdateRemoteRepositoryFails() {
        Set<Setting<?>> remoteStoreSettingSet = new HashSet<>();
        remoteStoreSettingSet.add(IndexMetadata.INDEX_REMOTE_STORE_REPOSITORY_SETTING);
        IndexScopedSettings settings = new IndexScopedSettings(Settings.EMPTY, remoteStoreSettingSet);
        SettingsException error = expectThrows(
            SettingsException.class,
            () -> settings.updateSettings(
                Settings.builder().put("index.remote_store.repository", randomUnicodeOfLength(10)).build(),
                Settings.builder(),
                Settings.builder(),
                "index"
            )
        );
        assertEquals(error.getMessage(), "final index setting [index.remote_store.repository], not updateable");
    }

    public void testSetRemoteRepositoryFailsWhenRemoteStoreIsNotEnabled() {
        Settings indexSettings = Settings.builder()
            .put("index.replication.type", ReplicationType.SEGMENT)
            .put("index.remote_store.enabled", false)
            .put("index.remote_store.repository", "repo1")
            .build();
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> IndexMetadata.INDEX_REMOTE_STORE_REPOSITORY_SETTING.get(indexSettings)
        );
        assertEquals(
            "Settings index.remote_store.repository can ont be set/enabled when index.remote_store.enabled is set to true",
            iae.getMessage()
        );
    }

    public void testSetRemoteRepositoryFailsWhenEmptyString() {
        Settings indexSettings = Settings.builder()
            .put("index.replication.type", ReplicationType.SEGMENT)
            .put("index.remote_store.enabled", false)
            .put("index.remote_store.repository", "")
            .build();
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> IndexMetadata.INDEX_REMOTE_STORE_REPOSITORY_SETTING.get(indexSettings)
        );
        assertEquals("Setting index.remote_store.repository should be provided with non-empty repository ID", iae.getMessage());
    }

    public void testRemoteTranslogRepoDefaultSetting() {
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertNull(settings.getRemoteStoreRepository());
    }

    public void testRemoteTranslogExplicitSetting() {
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_ENABLED, true)
                .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "tlog-store")
                .put(IndexMetadata.INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey(), "200ms")
                .build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertNull(settings.getRemoteStoreRepository());
        assertEquals("tlog-store", settings.getRemoteStoreTranslogRepository());
        assertEquals(TimeValue.timeValueMillis(200), settings.getRemoteTranslogUploadBufferInterval());
    }

    public void testSetRemoteTranslogRepositoryFailsWhenRemoteTranslogIsNotEnabled() {
        Settings indexSettings = Settings.builder()
            .put("index.replication.type", ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_ENABLED, false)
            .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "repo1")
            .build();
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> IndexMetadata.INDEX_REMOTE_TRANSLOG_REPOSITORY_SETTING.get(indexSettings)
        );
        assertEquals(
            "Settings index.remote_store.translog.repository can only be set/enabled when index.remote_store.translog.enabled is set to true",
            iae.getMessage()
        );
    }

    public void testSetRemoteTranslogRepositoryFailsWhenEmptyString() {
        Settings indexSettings = Settings.builder()
            .put("index.replication.type", ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_ENABLED, true)
            .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "")
            .build();
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> IndexMetadata.INDEX_REMOTE_TRANSLOG_REPOSITORY_SETTING.get(indexSettings)
        );
        assertEquals("Setting index.remote_store.translog.repository should be provided with non-empty repository ID", iae.getMessage());
    }

    public void testSetRemoteTranslogBufferIntervalDefaultSetting() {
        Version createdVersion = VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.CURRENT);
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), createdVersion)
            .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_ENABLED, true)
            .build();
        assertEquals(TimeValue.timeValueMillis(100), IndexMetadata.INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.get(settings));
    }

    public void testSetRemoteTranslogBufferIntervalFailsWhenRemoteTranslogIsNotEnabled() {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_ENABLED, false)
            .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_BUFFER_INTERVAL, "200ms")
            .build();
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> IndexMetadata.INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.get(indexSettings)
        );
        assertEquals(
            "Setting index.remote_store.translog.buffer_interval can only be set when index.remote_store.translog.enabled is set to true",
            iae.getMessage()
        );
    }

    public void testSetRemoteTranslogBufferIntervalFailsWhenEmpty() {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_ENABLED, false)
            .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_BUFFER_INTERVAL, "")
            .build();
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> IndexMetadata.INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.get(indexSettings)
        );
        assertEquals(
            "failed to parse setting [index.remote_store.translog.buffer_interval] with value [] as a time value: unit is missing or unrecognized",
            iae.getMessage()
        );
    }

    @SuppressForbidden(reason = "sets the SEARCHABLE_SNAPSHOT_EXTENDED_COMPATIBILITY feature flag")
    public void testExtendedCompatibilityVersionForRemoteSnapshot() throws Exception {
        try (FeatureFlagSetter f = FeatureFlagSetter.set(FeatureFlags.SEARCHABLE_SNAPSHOT_EXTENDED_COMPATIBILITY)) {
            IndexMetadata metadata = newIndexMeta(
                "index",
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.REMOTE_SNAPSHOT.getSettingsKey())
                    .build()
            );
            IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
            assertTrue(settings.isRemoteSnapshot());
            assertEquals(SEARCHABLE_SNAPSHOT_EXTENDED_COMPATIBILITY_MINIMUM_VERSION, settings.getExtendedCompatibilitySnapshotVersion());
        }
    }

    public void testExtendedCompatibilityVersionForNonRemoteSnapshot() {
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.FS.getSettingsKey())
                .build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertFalse(settings.isRemoteSnapshot());
        assertEquals(Version.CURRENT.minimumIndexCompatibilityVersion(), settings.getExtendedCompatibilitySnapshotVersion());
    }

    public void testExtendedCompatibilityVersionWithoutFeatureFlag() {
        IndexMetadata metadata = newIndexMeta(
            "index",
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.REMOTE_SNAPSHOT.getSettingsKey())
                .build()
        );
        IndexSettings settings = new IndexSettings(metadata, Settings.EMPTY);
        assertTrue(settings.isRemoteSnapshot());
        assertEquals(Version.CURRENT.minimumIndexCompatibilityVersion(), settings.getExtendedCompatibilitySnapshotVersion());
    }
}
