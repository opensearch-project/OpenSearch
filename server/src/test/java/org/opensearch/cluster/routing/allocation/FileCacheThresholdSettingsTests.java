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
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;

public class FileCacheThresholdSettingsTests extends OpenSearchTestCase {

    public void testDefaults() {
        ClusterSettings nss = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        FileCacheThresholdSettings fileCacheThreshldSettings = new FileCacheThresholdSettings(Settings.EMPTY, nss);

        assertEquals("90.0", fileCacheThreshldSettings.getFileCacheIndexThreshold().toString());
        assertEquals("100.0", fileCacheThreshldSettings.getFileCacheSearchThreshold().toString());
    }

    public void testInvalidIndexThreshold() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new FileCacheThresholdSettings(Settings.EMPTY, clusterSettings); // this has the effect of registering the settings updater

        final Settings newSettings = Settings.builder()
            .put(FileCacheThresholdSettings.CLUSTER_ROUTING_ALLOCATION_FILECACHE_INDEX_THRESHOLD_SETTING.getKey(), "96.0%")
            .put(FileCacheThresholdSettings.CLUSTER_ROUTING_ALLOCATION_FILECACHE_SEARCH_THRESHOLD_SETTING.getKey(), "95.0%")
            .build();

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> clusterSettings.applySettings(newSettings));
        final String expected = "illegal value can't update [cluster.routing.allocation.filecache.index.threshold] from [90%] to [96.0%]";
        assertThat(e, hasToString(containsString(expected)));
        assertNotNull(e.getCause());
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        final IllegalArgumentException cause = (IllegalArgumentException) e.getCause();
        assertThat(cause, hasToString(containsString("index file cache threshold [96.0%] more than search file cache threshold [95.0%]")));
    }

    public void testSequenceOfUpdates() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new FileCacheThresholdSettings(Settings.EMPTY, clusterSettings); // this has the effect of registering the settings updater

        final Settings.Builder target = Settings.builder();
        {
            assertNull(target.get(FileCacheThresholdSettings.CLUSTER_ROUTING_ALLOCATION_FILECACHE_INDEX_THRESHOLD_SETTING.getKey()));
            assertNull(target.get(FileCacheThresholdSettings.CLUSTER_ROUTING_ALLOCATION_FILECACHE_SEARCH_THRESHOLD_SETTING.getKey()));
        }

        {
            final Settings settings = Settings.builder()
                .put(FileCacheThresholdSettings.CLUSTER_ROUTING_ALLOCATION_FILECACHE_INDEX_THRESHOLD_SETTING.getKey(), "95.0%")
                .build();
            final Settings.Builder updates = Settings.builder();
            assertTrue(clusterSettings.updateSettings(settings, target, updates, "transient"));
            assertEquals(
                "95.0%",
                target.get(FileCacheThresholdSettings.CLUSTER_ROUTING_ALLOCATION_FILECACHE_INDEX_THRESHOLD_SETTING.getKey())
            );
            assertNull(target.get(FileCacheThresholdSettings.CLUSTER_ROUTING_ALLOCATION_FILECACHE_SEARCH_THRESHOLD_SETTING.getKey()));
        }

        {
            final Settings settings = Settings.builder()
                .put(FileCacheThresholdSettings.CLUSTER_ROUTING_ALLOCATION_FILECACHE_SEARCH_THRESHOLD_SETTING.getKey(), "97.0%")
                .build();
            final Settings.Builder updates = Settings.builder();
            assertTrue(clusterSettings.updateSettings(settings, target, updates, "transient"));
            assertEquals(
                "95.0%",
                target.get(FileCacheThresholdSettings.CLUSTER_ROUTING_ALLOCATION_FILECACHE_INDEX_THRESHOLD_SETTING.getKey())
            );
            assertEquals(
                "97.0%",
                target.get(FileCacheThresholdSettings.CLUSTER_ROUTING_ALLOCATION_FILECACHE_SEARCH_THRESHOLD_SETTING.getKey())
            );
        }
    }

    public void testThresholdDescriptions() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        FileCacheThresholdSettings fileCacheThreshldSettings = new FileCacheThresholdSettings(Settings.EMPTY, clusterSettings);
        assertEquals("90.0%", fileCacheThreshldSettings.describeIndexThreshold());
        assertEquals("100.0%", fileCacheThreshldSettings.describeSearchThreshold());
        fileCacheThreshldSettings = new FileCacheThresholdSettings(
            Settings.builder()
                .put(FileCacheThresholdSettings.CLUSTER_ROUTING_ALLOCATION_FILECACHE_INDEX_THRESHOLD_SETTING.getKey(), "91.1%")
                .put(FileCacheThresholdSettings.CLUSTER_ROUTING_ALLOCATION_FILECACHE_SEARCH_THRESHOLD_SETTING.getKey(), "91.2%")
                .build(),
            clusterSettings
        );
        assertEquals("91.1%", fileCacheThreshldSettings.describeIndexThreshold());
        assertEquals("91.2%", fileCacheThreshldSettings.describeSearchThreshold());
    }

}
