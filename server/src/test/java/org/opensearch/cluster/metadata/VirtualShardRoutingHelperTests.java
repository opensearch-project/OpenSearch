/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

public class VirtualShardRoutingHelperTests extends OpenSearchTestCase {

    public void testResolvePhysicalShardIdDefaultRangeBased() {
        int numPhysicalShards = 5;
        IndexMetadata metadata = IndexMetadata.builder("test")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_VIRTUAL_SHARDS, 20)
            )
            .numberOfShards(numPhysicalShards)
            .numberOfReplicas(1)
            .build();

        assertEquals(0, VirtualShardRoutingHelper.resolvePhysicalShardId(metadata, 0));
        assertEquals(0, VirtualShardRoutingHelper.resolvePhysicalShardId(metadata, 3));
        assertEquals(1, VirtualShardRoutingHelper.resolvePhysicalShardId(metadata, 4));
        assertEquals(1, VirtualShardRoutingHelper.resolvePhysicalShardId(metadata, 7));
        assertEquals(4, VirtualShardRoutingHelper.resolvePhysicalShardId(metadata, 19));
    }

    public void testResolvePhysicalShardIdWithOverrides() {
        int numPhysicalShards = 5;
        Map<String, String> overrides = new HashMap<>();
        overrides.put("7", "1"); // mapped out of standard routing
        overrides.put("8", "2"); // mapped out of standard routing

        IndexMetadata.Builder builder = IndexMetadata.builder("test")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_VIRTUAL_SHARDS, 20)
            )
            .numberOfShards(numPhysicalShards)
            .numberOfReplicas(1);
        builder.putCustom(VirtualShardRoutingHelper.VIRTUAL_SHARDS_CUSTOM_METADATA_KEY, overrides);

        IndexMetadata metadata = builder.build();

        assertEquals(1, VirtualShardRoutingHelper.resolvePhysicalShardId(metadata, 7));
        assertEquals(2, VirtualShardRoutingHelper.resolvePhysicalShardId(metadata, 8));

        // Default falls back to range-based formula (20 / 5 = 4 vshards per pshard)
        assertEquals(0, VirtualShardRoutingHelper.resolvePhysicalShardId(metadata, 0));
        assertEquals(2, VirtualShardRoutingHelper.resolvePhysicalShardId(metadata, 9));
    }

    public void testInvalidOverridesFallBackToRangeBased() {
        int numPhysicalShards = 5;
        Map<String, String> overrides = new HashMap<>();
        overrides.put("7", "not_a_number");
        overrides.put("8", "-1");
        overrides.put("19", "5");

        IndexMetadata.Builder builder = IndexMetadata.builder("test")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_VIRTUAL_SHARDS, 20)
            )
            .numberOfShards(numPhysicalShards)
            .numberOfReplicas(1);
        builder.putCustom(VirtualShardRoutingHelper.VIRTUAL_SHARDS_CUSTOM_METADATA_KEY, overrides);

        IndexMetadata metadata = builder.build();

        // Standard range-based routing expects 4 vshards per physical shard
        // vShard 7 -> 7 / 4 = 1
        assertEquals(1, VirtualShardRoutingHelper.resolvePhysicalShardId(metadata, 7));
        // vShard 8 -> 8 / 4 = 2
        assertEquals(2, VirtualShardRoutingHelper.resolvePhysicalShardId(metadata, 8));
        // vShard 19 -> 19 / 4 = 4
        assertEquals(4, VirtualShardRoutingHelper.resolvePhysicalShardId(metadata, 19));
    }
}
