/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.catalog;

import org.opensearch.catalog.CatalogPublishesInProgress;
import org.opensearch.catalog.PublishEntry;
import org.opensearch.catalog.PublishPhase;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class TransportPublishIndexActionTests extends OpenSearchTestCase {

    private static final String INDEX_NAME = "logs";
    private static final String INDEX_UUID = "uuid-logs";

    public void testAddEntryCreatesEntryInInitializedPhase() {
        ClusterState state = stateWithIndex(INDEX_NAME, INDEX_UUID);
        ClusterState next = TransportPublishIndexAction.addEntry(state, "publish-1", INDEX_NAME, INDEX_UUID, 10);

        CatalogPublishesInProgress custom = next.metadata().custom(CatalogPublishesInProgress.TYPE);
        assertNotNull(custom);
        assertEquals(1, custom.entries().size());
        PublishEntry entry = custom.entries().get(0);
        assertEquals("publish-1", entry.publishId());
        assertEquals(INDEX_NAME, entry.indexName());
        assertEquals(PublishPhase.INITIALIZED, entry.phase());
        assertTrue(entry.shardStatuses().isEmpty());
    }

    public void testAddEntryRejectsDuplicateSubmit() {
        PublishEntry existing = PublishEntry.builder()
            .publishId("existing").indexName(INDEX_NAME).indexUUID(INDEX_UUID)
            .startedAt(1L).build();
        ClusterState state = stateWithIndex(INDEX_NAME, INDEX_UUID, List.of(existing));

        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> TransportPublishIndexAction.addEntry(state, "new", INDEX_NAME, INDEX_UUID, 10)
        );
        assertTrue(ex.getMessage(), ex.getMessage().contains("publish already in progress"));
    }

    public void testAddEntryRejectsOverMaxConcurrent() {
        PublishEntry e1 = PublishEntry.builder()
            .publishId("e1").indexName("a").indexUUID("u-a").startedAt(1L).build();
        PublishEntry e2 = PublishEntry.builder()
            .publishId("e2").indexName("b").indexUUID("u-b").startedAt(1L).build();
        ClusterState state = stateWithIndex(INDEX_NAME, INDEX_UUID, List.of(e1, e2));

        OpenSearchRejectedExecutionException ex = expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> TransportPublishIndexAction.addEntry(state, "new", INDEX_NAME, INDEX_UUID, 2)
        );
        assertTrue(ex.getMessage(), ex.getMessage().contains("max concurrent"));
    }

    public void testAddEntryRejectsWhenIndexDisappeared() {
        ClusterState state = ClusterState.builder(new ClusterName("test")).build();
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> TransportPublishIndexAction.addEntry(state, "publish-1", INDEX_NAME, INDEX_UUID, 10)
        );
        assertTrue(ex.getMessage(), ex.getMessage().contains("not found"));
    }

    private static ClusterState stateWithIndex(String indexName, String indexUUID) {
        return stateWithIndex(indexName, indexUUID, List.of());
    }

    private static ClusterState stateWithIndex(String indexName, String indexUUID, List<PublishEntry> existing) {
        IndexMetadata im = IndexMetadata.builder(indexName)
            .settings(Settings.builder()
                .put(IndexMetadata.SETTING_INDEX_UUID, indexUUID)
                .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
            .build();
        Metadata.Builder mb = Metadata.builder().put(im, false);
        if (!existing.isEmpty()) {
            mb.putCustom(CatalogPublishesInProgress.TYPE, new CatalogPublishesInProgress(existing));
        }
        return ClusterState.builder(new ClusterName("test")).metadata(mb.build()).build();
    }
}
