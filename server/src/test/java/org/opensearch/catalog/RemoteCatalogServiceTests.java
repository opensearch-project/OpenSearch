/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.catalog;

import org.opensearch.action.admin.cluster.catalog.PublishShardResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoteCatalogServiceTests extends OpenSearchTestCase {

    private MetadataClient metadataClient;
    private ClusterService clusterService;
    private Client client;
    private RemoteCatalogService service;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        metadataClient = mock(MetadataClient.class);
        clusterService = mock(ClusterService.class);
        client = mock(Client.class);
        service = new RemoteCatalogService(clusterService, client, metadataClient);
    }

    public void testPublishIndexFailsWhenNoMetadataClient() {
        RemoteCatalogService serviceWithoutClient = new RemoteCatalogService(clusterService, client, null);

        AtomicReference<Exception> failure = new AtomicReference<>();
        serviceWithoutClient.publishIndex("my-index", new ActionListener<>() {
            @Override
            public void onResponse(PublishShardResponse response) {
                fail("expected failure");
            }

            @Override
            public void onFailure(Exception e) {
                failure.set(e);
            }
        });

        assertNotNull(failure.get());
        assertTrue(failure.get() instanceof IllegalStateException);
        assertTrue(failure.get().getMessage().contains("catalog is not configured"));
    }

    public void testPublishIndexFailsWhenIndexNotFound() {
        ClusterState state = ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder().build()).build();
        when(clusterService.state()).thenReturn(state);

        AtomicReference<Exception> failure = new AtomicReference<>();
        service.publishIndex("nonexistent-index", new ActionListener<>() {
            @Override
            public void onResponse(PublishShardResponse response) {
                fail("expected failure");
            }

            @Override
            public void onFailure(Exception e) {
                failure.set(e);
            }
        });

        assertNotNull(failure.get());
        assertTrue(failure.get() instanceof IllegalArgumentException);
        assertTrue(failure.get().getMessage().contains("not found in cluster state"));
    }

    public void testPublishIndexFailsWhenInitializeFails() throws IOException {
        IndexMetadata indexMetadata = IndexMetadata.builder("my-index")
            .settings(Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
            .build();
        ClusterState state = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().put(indexMetadata, false).build())
            .build();
        when(clusterService.state()).thenReturn(state);

        doThrow(new IOException("catalog unavailable")).when(metadataClient).initialize(eq("my-index"), any());

        AtomicReference<Exception> failure = new AtomicReference<>();
        service.publishIndex("my-index", new ActionListener<>() {
            @Override
            public void onResponse(PublishShardResponse response) {
                fail("expected failure");
            }

            @Override
            public void onFailure(Exception e) {
                failure.set(e);
            }
        });

        assertNotNull(failure.get());
        assertTrue(failure.get() instanceof IOException);
        verify(client, never()).execute(any(), any(), any());
    }
}
