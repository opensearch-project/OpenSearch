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
        overrides.put("7", "1");
        overrides.put("8", "2");

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

        assertEquals(1, VirtualShardRoutingHelper.resolvePhysicalShardId(metadata, 7));
        assertEquals(2, VirtualShardRoutingHelper.resolvePhysicalShardId(metadata, 8));
        assertEquals(4, VirtualShardRoutingHelper.resolvePhysicalShardId(metadata, 19));
    }

    public void testResolvePhysicalShardIdInvalidConfigurations() {
        int numPhysicalShards = 5;

        IndexMetadata metadataDisabled = org.mockito.Mockito.mock(IndexMetadata.class);
        org.mockito.Mockito.when(metadataDisabled.getNumberOfVirtualShards()).thenReturn(-1);
        org.mockito.Mockito.when(metadataDisabled.getNumberOfShards()).thenReturn(numPhysicalShards);

        IllegalArgumentException e1 = expectThrows(
            IllegalArgumentException.class,
            () -> VirtualShardRoutingHelper.resolvePhysicalShardId(metadataDisabled, 0)
        );
        assertTrue(e1.getMessage().contains("must be enabled and be a multiple"));

        IndexMetadata metadataInvalid = org.mockito.Mockito.mock(IndexMetadata.class);
        org.mockito.Mockito.when(metadataInvalid.getNumberOfVirtualShards()).thenReturn(13);
        org.mockito.Mockito.when(metadataInvalid.getNumberOfShards()).thenReturn(numPhysicalShards);

        IllegalArgumentException e2 = expectThrows(
            IllegalArgumentException.class,
            () -> VirtualShardRoutingHelper.resolvePhysicalShardId(metadataInvalid, 0)
        );
        assertTrue(e2.getMessage().contains("must be enabled and be a multiple"));
    }

    public void testResolvePhysicalShardIdOutOfBoundsNormalization() {
        int numPhysicalShards = 5;
        IndexMetadata metadata = org.mockito.Mockito.mock(IndexMetadata.class);
        org.mockito.Mockito.when(metadata.getNumberOfVirtualShards()).thenReturn(20);
        org.mockito.Mockito.when(metadata.getNumberOfShards()).thenReturn(numPhysicalShards);

        assertEquals(4, VirtualShardRoutingHelper.resolvePhysicalShardId(metadata, -1));

        assertEquals(0, VirtualShardRoutingHelper.resolvePhysicalShardId(metadata, 20));
        assertEquals(1, VirtualShardRoutingHelper.resolvePhysicalShardId(metadata, 25));
    }
}
