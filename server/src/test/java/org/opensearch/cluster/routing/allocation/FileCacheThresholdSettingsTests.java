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
package org.opensearch.cluster.routing.allocation;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.test.OpenSearchTestCase;

public class FileCacheThresholdSettingsTests extends OpenSearchTestCase {

    public void testDefaults() {
        ClusterSettings nss = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        FileCacheThresholdSettings fileCacheThresholdSettings = new FileCacheThresholdSettings(Settings.EMPTY, nss);
        ByteSizeValue zeroBytes = ByteSizeValue.parseBytesSizeValue("0b", "test");

        assertEquals("90.0", fileCacheThresholdSettings.getFileCacheIndexThresholdPercentage().toString());
        assertEquals("100.0", fileCacheThresholdSettings.getFileCacheSearchThresholdPercentage().toString());
        assertEquals(zeroBytes, fileCacheThresholdSettings.getFileCacheIndexThresholdBytes());
        assertEquals(zeroBytes, fileCacheThresholdSettings.getFileCacheSearchThresholdBytes());
    }

    public void testUpdateWithBytes() {
        ClusterSettings nss = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        FileCacheThresholdSettings fileCacheThresholdSettings = new FileCacheThresholdSettings(Settings.EMPTY, nss);

        Settings newSettings = Settings.builder()
            .put(FileCacheThresholdSettings.CLUSTER_FILECACHE_ACTIVEUSAGE_INDEXING_THRESHOLD_SETTING.getKey(), "500mb")
            .put(FileCacheThresholdSettings.CLUSTER_FILECACHE_ACTIVEUSAGE_SEARCH_THRESHOLD_SETTING.getKey(), "1000mb")
            .build();
        nss.applySettings(newSettings);

        assertEquals(ByteSizeValue.parseBytesSizeValue("500mb", "test"), fileCacheThresholdSettings.getFileCacheIndexThresholdBytes());
        assertEquals(ByteSizeValue.parseBytesSizeValue("1000mb", "test"), fileCacheThresholdSettings.getFileCacheSearchThresholdBytes());
        assertEquals("0.0", fileCacheThresholdSettings.getFileCacheIndexThresholdPercentage().toString());
        assertEquals("0.0", fileCacheThresholdSettings.getFileCacheSearchThresholdPercentage().toString());
    }

    public void testInvalidIndexThreshold() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new FileCacheThresholdSettings(Settings.EMPTY, clusterSettings); // this has the effect of registering the settings updater

        final Settings newSettings = Settings.builder()
            .put(FileCacheThresholdSettings.CLUSTER_FILECACHE_ACTIVEUSAGE_INDEXING_THRESHOLD_SETTING.getKey(), "96.0%")
            .put(FileCacheThresholdSettings.CLUSTER_FILECACHE_ACTIVEUSAGE_SEARCH_THRESHOLD_SETTING.getKey(), "95.0%")
            .build();

        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> clusterSettings.applySettings(newSettings));
        final String expected = "illegal value can't update [cluster.filecache.activeusage.indexing.threshold] from [90%] to [96.0%]";
        assertTrue(e.getMessage().contains(expected));
        assertNotNull(e.getCause());
        assertTrue(e.getCause() instanceof IllegalArgumentException);
        final IllegalArgumentException cause = (IllegalArgumentException) e.getCause();
        assertTrue(cause.getMessage().contains("index file cache threshold [96.0%] more than search file cache threshold [95.0%]"));
    }

    public void testSequenceOfUpdates() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new FileCacheThresholdSettings(Settings.EMPTY, clusterSettings); // this has the effect of registering the settings updater

        final Settings.Builder target = Settings.builder();
        {
            assertNull(target.get(FileCacheThresholdSettings.CLUSTER_FILECACHE_ACTIVEUSAGE_INDEXING_THRESHOLD_SETTING.getKey()));
            assertNull(target.get(FileCacheThresholdSettings.CLUSTER_FILECACHE_ACTIVEUSAGE_SEARCH_THRESHOLD_SETTING.getKey()));
        }

        {
            final Settings settings = Settings.builder()
                .put(FileCacheThresholdSettings.CLUSTER_FILECACHE_ACTIVEUSAGE_INDEXING_THRESHOLD_SETTING.getKey(), "95.0%")
                .build();
            final Settings.Builder updates = Settings.builder();
            assertTrue(clusterSettings.updateSettings(settings, target, updates, "transient"));
            assertEquals("95.0%", target.get(FileCacheThresholdSettings.CLUSTER_FILECACHE_ACTIVEUSAGE_INDEXING_THRESHOLD_SETTING.getKey()));
            assertNull(target.get(FileCacheThresholdSettings.CLUSTER_FILECACHE_ACTIVEUSAGE_SEARCH_THRESHOLD_SETTING.getKey()));
        }

        {
            final Settings settings = Settings.builder()
                .put(FileCacheThresholdSettings.CLUSTER_FILECACHE_ACTIVEUSAGE_SEARCH_THRESHOLD_SETTING.getKey(), "97.0%")
                .build();
            final Settings.Builder updates = Settings.builder();
            assertTrue(clusterSettings.updateSettings(settings, target, updates, "transient"));
            assertEquals("95.0%", target.get(FileCacheThresholdSettings.CLUSTER_FILECACHE_ACTIVEUSAGE_INDEXING_THRESHOLD_SETTING.getKey()));
            assertEquals("97.0%", target.get(FileCacheThresholdSettings.CLUSTER_FILECACHE_ACTIVEUSAGE_SEARCH_THRESHOLD_SETTING.getKey()));
        }
    }

    public void testThresholdDescriptions() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        FileCacheThresholdSettings fileCacheThreshldSettings = new FileCacheThresholdSettings(Settings.EMPTY, clusterSettings);
        assertEquals("90.0%", fileCacheThreshldSettings.describeIndexThreshold());
        assertEquals("100.0%", fileCacheThreshldSettings.describeSearchThreshold());
        fileCacheThreshldSettings = new FileCacheThresholdSettings(
            Settings.builder()
                .put(FileCacheThresholdSettings.CLUSTER_FILECACHE_ACTIVEUSAGE_INDEXING_THRESHOLD_SETTING.getKey(), "91.1%")
                .put(FileCacheThresholdSettings.CLUSTER_FILECACHE_ACTIVEUSAGE_SEARCH_THRESHOLD_SETTING.getKey(), "91.2%")
                .build(),
            clusterSettings
        );
        assertEquals("91.1%", fileCacheThreshldSettings.describeIndexThreshold());
        assertEquals("91.2%", fileCacheThreshldSettings.describeSearchThreshold());
    }

}
