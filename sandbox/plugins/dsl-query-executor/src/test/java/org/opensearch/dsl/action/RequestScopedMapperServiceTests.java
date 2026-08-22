/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.action;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class RequestScopedMapperServiceTests extends OpenSearchTestCase {

    private static final String INDEX = "probe";

    private ClusterState stateWithIndex() {
        IndexMetadata indexMetadata = IndexMetadata.builder(INDEX)
            .settings(
                Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        return ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder().put(indexMetadata, false)).build();
    }

    /**
     * The load-bearing regression test: a MapperService fresh out of
     * {@code createIndexMapperService} is empty, so the holder must merge the index mapping
     * before handing the service to response building — otherwise every fieldType() lookup
     * returns null and terms responses silently degrade to sampled typing.
     */
    public void testMergesIndexMappingIntoCreatedMapperService() {
        ClusterState state = stateWithIndex();
        IndexMetadata indexMetadata = state.metadata().index(INDEX);
        MapperService created = mock(MapperService.class);

        try (RequestScopedMapperService holder = new RequestScopedMapperService(INDEX, () -> state, metadata -> created)) {
            assertSame(created, holder.get());
            verify(created).merge(eq(indexMetadata), eq(MapperService.MergeReason.MAPPING_RECOVERY));
        }
    }

    public void testCreatesAtMostOneMapperServicePerRequest() {
        ClusterState state = stateWithIndex();
        MapperService created = mock(MapperService.class);
        AtomicInteger creations = new AtomicInteger();

        try (RequestScopedMapperService holder = new RequestScopedMapperService(INDEX, () -> state, metadata -> {
            creations.incrementAndGet();
            return created;
        })) {
            assertSame(created, holder.get());
            assertSame(created, holder.get());
            assertEquals("supplier must memoize the MapperService within a request", 1, creations.get());
        }
    }

    public void testReturnsNullWhenIndexMissingFromClusterState() {
        ClusterState empty = ClusterState.builder(new ClusterName("test")).build();
        AtomicInteger creations = new AtomicInteger();

        try (RequestScopedMapperService holder = new RequestScopedMapperService(INDEX, () -> empty, metadata -> {
            creations.incrementAndGet();
            return mock(MapperService.class);
        })) {
            assertNull(holder.get());
            assertEquals("factory must not run for a deleted index", 0, creations.get());
        }
    }

    public void testReturnsNullWhenCreationFails() {
        ClusterState state = stateWithIndex();

        try (RequestScopedMapperService holder = new RequestScopedMapperService(INDEX, () -> state, metadata -> {
            throw new IOException("simulated createIndexMapperService failure");
        })) {
            assertNull(holder.get());
            assertNull("failure must be memoized, not retried", holder.get());
        }
    }

    public void testClosesCreatedServiceWhenMergeFails() throws IOException {
        ClusterState state = stateWithIndex();
        MapperService created = mock(MapperService.class);
        doThrow(new IllegalArgumentException("simulated merge failure")).when(created)
            .merge(eq(state.metadata().index(INDEX)), eq(MapperService.MergeReason.MAPPING_RECOVERY));

        try (RequestScopedMapperService holder = new RequestScopedMapperService(INDEX, () -> state, metadata -> created)) {
            assertNull(holder.get());
            verify(created).close();
        }
    }

    public void testCloseReleasesMapperServiceAndSubsequentGetReturnsNull() throws IOException {
        ClusterState state = stateWithIndex();
        MapperService created = mock(MapperService.class);

        RequestScopedMapperService holder = new RequestScopedMapperService(INDEX, () -> state, metadata -> created);
        assertSame(created, holder.get());

        holder.close();
        verify(created).close();
        assertNull("a closed holder must not resurrect the MapperService", holder.get());

        holder.close(); // second close is a no-op
    }

    public void testCloseBeforeGetIsNoOp() throws IOException {
        ClusterState state = stateWithIndex();
        MapperService created = mock(MapperService.class);
        AtomicInteger creations = new AtomicInteger();

        RequestScopedMapperService holder = new RequestScopedMapperService(INDEX, () -> state, metadata -> {
            creations.incrementAndGet();
            return created;
        });
        holder.close();

        assertEquals(0, creations.get());
        verify(created, never()).close();
        assertNull(holder.get());
    }
}
