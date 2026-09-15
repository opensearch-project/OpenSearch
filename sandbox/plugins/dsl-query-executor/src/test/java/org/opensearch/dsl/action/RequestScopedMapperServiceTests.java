/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.action;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
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

    private IndexMetadata indexMetadata() {
        return IndexMetadata.builder(INDEX)
            .settings(
                Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }

    /**
     * The load-bearing regression test: a MapperService fresh out of
     * {@code createIndexMapperService} is empty, so the holder must merge the pinned index
     * mapping before handing the service to response building — otherwise every fieldType()
     * lookup returns null and response building fails on the first typed key.
     */
    public void testMergesIndexMappingIntoCreatedMapperService() {
        IndexMetadata indexMetadata = indexMetadata();
        MapperService created = mock(MapperService.class);

        try (RequestScopedMapperService holder = new RequestScopedMapperService(indexMetadata, metadata -> created)) {
            assertSame(created, holder.get());
            verify(created).merge(eq(indexMetadata), eq(MapperService.MergeReason.MAPPING_RECOVERY));
        }
    }

    public void testCreatesAtMostOneMapperServicePerRequest() {
        MapperService created = mock(MapperService.class);
        AtomicInteger creations = new AtomicInteger();

        try (RequestScopedMapperService holder = new RequestScopedMapperService(indexMetadata(), metadata -> {
            creations.incrementAndGet();
            return created;
        })) {
            assertSame(created, holder.get());
            assertSame(created, holder.get());
            assertEquals("supplier must memoize the MapperService within a request", 1, creations.get());
        }
    }

    public void testRejectsNullIndexMetadata() {
        expectThrows(NullPointerException.class, () -> new RequestScopedMapperService(null, metadata -> mock(MapperService.class)));
    }

    public void testReturnsNullWhenCreationFails() {
        try (RequestScopedMapperService holder = new RequestScopedMapperService(indexMetadata(), metadata -> {
            throw new IOException("simulated createIndexMapperService failure");
        })) {
            assertNull(holder.get());
            assertNull("failure must be memoized, not retried", holder.get());
        }
    }

    public void testClosesCreatedServiceWhenMergeFails() throws IOException {
        IndexMetadata indexMetadata = indexMetadata();
        MapperService created = mock(MapperService.class);
        doThrow(new IllegalArgumentException("simulated merge failure")).when(created)
            .merge(eq(indexMetadata), eq(MapperService.MergeReason.MAPPING_RECOVERY));

        try (RequestScopedMapperService holder = new RequestScopedMapperService(indexMetadata, metadata -> created)) {
            assertNull(holder.get());
            verify(created).close();
        }
    }

    public void testCloseReleasesMapperServiceAndSubsequentGetReturnsNull() throws IOException {
        MapperService created = mock(MapperService.class);

        RequestScopedMapperService holder = new RequestScopedMapperService(indexMetadata(), metadata -> created);
        assertSame(created, holder.get());

        holder.close();
        verify(created).close();
        assertNull("a closed holder must not resurrect the MapperService", holder.get());

        holder.close(); // second close is a no-op
    }

    public void testCloseBeforeGetIsNoOp() throws IOException {
        MapperService created = mock(MapperService.class);
        AtomicInteger creations = new AtomicInteger();

        RequestScopedMapperService holder = new RequestScopedMapperService(indexMetadata(), metadata -> {
            creations.incrementAndGet();
            return created;
        });
        holder.close();

        assertEquals(0, creations.get());
        verify(created, never()).close();
        assertNull(holder.get());
    }
}
