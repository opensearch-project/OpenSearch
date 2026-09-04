/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedIndexMetadata;
import org.opensearch.indices.SystemIndexDescriptor;
import org.opensearch.indices.SystemIndices;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RemoteClusterStateIndexFilterTests extends OpenSearchTestCase {

    private static final String SYSTEM_INDEX = ".test-system-index";
    private static final String USER_INDEX = "user-index";

    private final RemoteClusterStateIndexFilter indexFilter = new RemoteClusterStateIndexFilter(
        new SystemIndices(Map.of("test-plugin", List.of(new SystemIndexDescriptor(SYSTEM_INDEX, "test system index"))))
    );

    public void testIncludeIndex() {
        assertTrue(indexFilter.includeIndex(USER_INDEX));
        assertFalse(indexFilter.includeIndex(SYSTEM_INDEX));
    }

    public void testFilterIndexMetadata() {
        IndexMetadata systemIndexMetadata = indexMetadata(SYSTEM_INDEX, "uuid-1");
        IndexMetadata userIndexMetadata = indexMetadata(USER_INDEX, "uuid-2");
        List<IndexMetadata> filtered = indexFilter.filterIndexMetadata(List.of(systemIndexMetadata, userIndexMetadata));
        assertEquals(1, filtered.size());
        assertEquals(USER_INDEX, filtered.get(0).getIndex().getName());
    }

    public void testFilterUploadedIndexMetadata() {
        UploadedIndexMetadata systemIndex = new UploadedIndexMetadata(SYSTEM_INDEX, "uuid-1", "system-file");
        UploadedIndexMetadata userIndex = new UploadedIndexMetadata(USER_INDEX, "uuid-2", "user-file");
        List<UploadedIndexMetadata> filtered = indexFilter.filterUploadedIndexMetadata(List.of(systemIndex, userIndex));
        assertEquals(1, filtered.size());
        assertEquals(USER_INDEX, filtered.get(0).getIndexName());
    }

    public void testRemoveSystemIndicesFromUploadedMetadataMap() {
        Map<String, UploadedIndexMetadata> uploaded = new HashMap<>();
        uploaded.put(SYSTEM_INDEX, new UploadedIndexMetadata(SYSTEM_INDEX, "uuid-1", "system-file"));
        uploaded.put(USER_INDEX, new UploadedIndexMetadata(USER_INDEX, "uuid-2", "user-file"));
        indexFilter.removeSystemIndicesFromUploadedMetadataMap(uploaded);
        assertEquals(1, uploaded.size());
        assertTrue(uploaded.containsKey(USER_INDEX));
    }

    public void testEmptySystemIndicesRegistryIncludesAllIndices() {
        RemoteClusterStateIndexFilter noSystemIndicesFilter = new RemoteClusterStateIndexFilter(new SystemIndices(Collections.emptyMap()));
        assertTrue(noSystemIndicesFilter.includeIndex(SYSTEM_INDEX));
        assertTrue(noSystemIndicesFilter.includeIndex(USER_INDEX));
    }

    private static IndexMetadata indexMetadata(String indexName, String indexUUID) {
        return new IndexMetadata.Builder(indexName).settings(
            org.opensearch.common.settings.Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, indexUUID)
                .build()
        ).numberOfShards(1).numberOfReplicas(0).build();
    }

}
