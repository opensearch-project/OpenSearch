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

    public void testResolvePhysicalShardIdDefaultModulo() {
        int numPhysicalShards = 5;
        IndexMetadata metadata = IndexMetadata.builder("test")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(numPhysicalShards)
            .numberOfReplicas(1)
            .build();

        assertEquals(0, VirtualShardRoutingHelper.resolvePhysicalShardId(metadata, 0));
        assertEquals(2, VirtualShardRoutingHelper.resolvePhysicalShardId(metadata, 7));
        assertEquals(4, VirtualShardRoutingHelper.resolvePhysicalShardId(metadata, 4));
        assertEquals(0, VirtualShardRoutingHelper.resolvePhysicalShardId(metadata, 5));
    }

    public void testResolvePhysicalShardIdWithOverrides() {
        int numPhysicalShards = 5;
        Map<String, String> overrides = new HashMap<>();
        overrides.put("7", "1");

        IndexMetadata.Builder builder = IndexMetadata.builder("test")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(numPhysicalShards)
            .numberOfReplicas(1);
        builder.putCustom(VirtualShardRoutingHelper.VIRTUAL_SHARDS_CUSTOM_METADATA_KEY, overrides);

        IndexMetadata metadata = builder.build();

        assertEquals(1, VirtualShardRoutingHelper.resolvePhysicalShardId(metadata, 7));

        assertEquals(0, VirtualShardRoutingHelper.resolvePhysicalShardId(metadata, 0));
    }

    public void testInvalidOverridesFallBackToModulo() {
        int numPhysicalShards = 5;
        Map<String, String> overrides = new HashMap<>();
        overrides.put("7", "not_a_number");
        overrides.put("8", "-1");
        overrides.put("9", "5");

        IndexMetadata.Builder builder = IndexMetadata.builder("test")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(numPhysicalShards)
            .numberOfReplicas(1);
        builder.putCustom(VirtualShardRoutingHelper.VIRTUAL_SHARDS_CUSTOM_METADATA_KEY, overrides);

        IndexMetadata metadata = builder.build();

        assertEquals(2, VirtualShardRoutingHelper.resolvePhysicalShardId(metadata, 7));
        assertEquals(3, VirtualShardRoutingHelper.resolvePhysicalShardId(metadata, 8));
        assertEquals(4, VirtualShardRoutingHelper.resolvePhysicalShardId(metadata, 9));
    }
}
