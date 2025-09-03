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

package org.opensearch.common.settings;

import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.indices.IndexingMemoryController;
import org.opensearch.indices.IndicesQueryCache;
import org.opensearch.indices.IndicesRequestCache;
import org.opensearch.indices.breaker.HierarchyCircuitBreakerService;
import org.opensearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.notNullValue;

public class MemorySizeSettingsTests extends OpenSearchTestCase {

    public void testPageCacheLimitHeapSetting() {
        assertMemorySizeSetting(
            PageCacheRecycler.LIMIT_HEAP_SETTING,
            "cache.recycler.page.limit.heap",
            new ByteSizeValue((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.1))
        );
    }

    public void testIndexBufferSizeSetting() {
        assertMemorySizeSetting(
            IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING,
            "indices.memory.index_buffer_size",
            new ByteSizeValue((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.1))
        );
    }

    public void testQueryCacheSizeSetting() {
        assertMemorySizeSetting(
            IndicesQueryCache.INDICES_CACHE_QUERY_SIZE_SETTING,
            "indices.queries.cache.size",
            new ByteSizeValue((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.1))
        );
    }

    public void testIndicesRequestCacheSetting() {
        assertMemorySizeSetting(
            IndicesRequestCache.INDICES_CACHE_QUERY_SIZE,
            "indices.requests.cache.size",
            new ByteSizeValue((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.01))
        );
        assertWarnings(
            "[indices.requests.cache.size] setting was deprecated in OpenSearch and will be removed in a future release! See the breaking changes documentation for the next major version."
        );
    }

    public void testCircuitBreakerSettings() {
        final Settings settings = Settings.builder()
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), randomBoolean())
            .build();

        // default is chosen based on USE_REAL_MEMORY_USAGE_SETTING setting
        double defaultTotalPercentage;
        if (HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.get(settings)) {
            defaultTotalPercentage = 0.95d;
        } else {
            defaultTotalPercentage = 0.7d;
        }
        assertMemorySizeSetting(
            HierarchyCircuitBreakerService.TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING,
            "indices.breaker.total.limit",
            new ByteSizeValue((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * defaultTotalPercentage)),
            settings
        );
        assertMemorySizeSetting(
            HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING,
            "indices.breaker.fielddata.limit",
            new ByteSizeValue((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.4)),
            settings
        );
        assertMemorySizeSetting(
            HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING,
            "indices.breaker.request.limit",
            new ByteSizeValue((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.6)),
            settings
        );
        assertMemorySizeSetting(
            HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING,
            "network.breaker.inflight_requests.limit",
            new ByteSizeValue((JvmInfo.jvmInfo().getMem().getHeapMax().getBytes())),
            settings
        );
    }

    public void testIndicesFieldDataCacheSetting() {
        assertMemorySizeSetting(
            IndicesFieldDataCache.INDICES_FIELDDATA_CACHE_SIZE_KEY,
            "indices.fielddata.cache.size",
            new ByteSizeValue(-1)
        );
    }

    public void testSnapshotRepositoryDataCacheSizeSetting() {
        assertMemorySizeSettingInRange(
            BlobStoreRepository.SNAPSHOT_REPOSITORY_DATA_CACHE_THRESHOLD,
            "snapshot.repository_data.cache.threshold",
            new ByteSizeValue(BlobStoreRepository.calculateDefaultSnapshotRepositoryDataCacheThreshold()),
            ByteSizeUnit.KB.toBytes(500),
            1.0
        );
    }

    public void testMinMaxSettingValues() {
        String key = "setting_key";
        double minPercentage = 0.5;
        double maxPercentage = 10;
        long minBytes = (long) (minPercentage * 0.01 * JvmInfo.jvmInfo().getMem().getHeapMax().getBytes());
        long maxBytes = (long) (maxPercentage * 0.01 * JvmInfo.jvmInfo().getMem().getHeapMax().getBytes());
        double defaultPercentage = 1;
        long defaultBytes = (long) (defaultPercentage * 0.01 * JvmInfo.jvmInfo().getMem().getHeapMax().getBytes());

        // Test inputs both as percents and byte strings
        Setting<ByteSizeValue> dummySettingWithPercents = Setting.memorySizeSetting(
            key,
            defaultPercentage + "%",
            minPercentage + "%",
            maxPercentage + "%",
            Property.Dynamic
        );
        Setting<ByteSizeValue> dummySettingWithBytes = Setting.memorySizeSetting(
            key,
            defaultBytes + "b",
            minBytes + "b",
            maxBytes + "b",
            Property.Dynamic
        );
        for (Setting<ByteSizeValue> dummySetting : List.of(dummySettingWithBytes, dummySettingWithPercents)) {
            assertThrows(IllegalArgumentException.class, () -> { dummySetting.get(Settings.builder().put(key, "0.49%").build()); });
            assertThrows(IllegalArgumentException.class, () -> { dummySetting.get(Settings.builder().put(key, "11%").build()); });

            assertThrows(
                IllegalArgumentException.class,
                () -> { dummySetting.get(Settings.builder().put(key, minBytes - 1 + "b").build()); }
            );
            assertThrows(
                IllegalArgumentException.class,
                () -> { dummySetting.get(Settings.builder().put(key, maxBytes + 1 + "b").build()); }
            );

            Settings settings = Settings.builder().put(key, "10%").build();
            ByteSizeValue value = dummySetting.get(settings);
            assertEquals(value.getBytes(), (long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.1));

            settings = Settings.builder().put(key, "0.5%").build();
            value = dummySetting.get(settings);
            assertEquals(value.getBytes(), (long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.005));

            assertEquals(dummySetting.get(Settings.EMPTY).getBytes(), defaultBytes);

            // Show we can't dynamically update it out of the bounds
            AtomicReference<ByteSizeValue> reference = new AtomicReference<>(null);
            ClusterSettings.SettingUpdater<ByteSizeValue> settingUpdater = dummySetting.newUpdater(reference::set, logger);
            assertThrows(
                IllegalArgumentException.class,
                () -> { settingUpdater.apply(Settings.builder().put(key, "11%").build(), Settings.EMPTY); }
            );
            assertThrows(
                IllegalArgumentException.class,
                () -> { settingUpdater.apply(Settings.builder().put(key, "0.49%").build(), Settings.EMPTY); }
            );
            assertThrows(IllegalArgumentException.class, () -> {
                settingUpdater.apply(Settings.builder().put(key, minBytes - 1 + "b").build(), Settings.EMPTY);
            });
            assertThrows(IllegalArgumentException.class, () -> {
                settingUpdater.apply(Settings.builder().put(key, maxBytes + 1 + "b").build(), Settings.EMPTY);
            });
        }
    }

    private void assertMemorySizeSetting(Setting<ByteSizeValue> setting, String settingKey, ByteSizeValue defaultValue) {
        assertMemorySizeSetting(setting, settingKey, defaultValue, Settings.EMPTY);
    }

    private void assertMemorySizeSetting(Setting<ByteSizeValue> setting, String settingKey, ByteSizeValue defaultValue, Settings settings) {
        assertMemorySizeSetting(setting, settingKey, defaultValue, 25.0, 1024, settings);
    }

    private void assertMemorySizeSetting(
        Setting<ByteSizeValue> setting,
        String settingKey,
        ByteSizeValue defaultValue,
        double availablePercentage,
        long availableBytes,
        Settings settings
    ) {
        assertThat(setting, notNullValue());
        assertThat(setting.getKey(), equalTo(settingKey));
        assertThat(setting.getProperties(), hasItem(Property.NodeScope));
        assertThat(setting.getDefault(settings), equalTo(defaultValue));
        Settings settingWithPercentage = Settings.builder().put(settingKey, percentageAsString(availablePercentage)).build();
        assertThat(
            setting.get(settingWithPercentage),
            equalTo(
                new ByteSizeValue((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * percentageAsFraction(availablePercentage)))
            )
        );
        Settings settingWithBytesValue = Settings.builder().put(settingKey, availableBytes + "b").build();
        assertThat(setting.get(settingWithBytesValue), equalTo(new ByteSizeValue(availableBytes)));
    }

    private void assertMemorySizeSettingInRange(
        Setting<ByteSizeValue> setting,
        String settingKey,
        ByteSizeValue defaultValue,
        long minBytes,
        double maxPercentage
    ) {
        assertMemorySizeSetting(setting, settingKey, defaultValue, maxPercentage, minBytes, Settings.EMPTY);

        assertThrows(IllegalArgumentException.class, () -> {
            Settings settingWithTooSmallValue = Settings.builder().put(settingKey, minBytes - 1).build();
            setting.get(settingWithTooSmallValue);
        });

        assertThrows(IllegalArgumentException.class, () -> {
            double unavailablePercentage = maxPercentage + 0.1;
            Settings settingWithPercentageExceedingLimit = Settings.builder()
                .put(settingKey, percentageAsString(unavailablePercentage))
                .build();
            setting.get(settingWithPercentageExceedingLimit);
        });
    }

    private double percentageAsFraction(double availablePercentage) {
        return availablePercentage / 100.0;
    }

    private String percentageAsString(double availablePercentage) {
        return availablePercentage + "%";
    }
}
