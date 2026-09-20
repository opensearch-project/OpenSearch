/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file to be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.IndicesAdminClient;

import java.util.Map;

import org.mockito.ArgumentCaptor;

import static org.hamcrest.Matchers.arrayContaining;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class SystemIndexMappingUpdaterTests extends OpenSearchTestCase {
    private static final String INDEX_PATTERN = ".test-*";
    private static final String INDEX_NAME = ".test-index";
    private static final String WRITE_ALIAS = ".test-write";

    public void testUpdatesOlderConcreteIndexMapping() throws Exception {
        SystemIndexDescriptor descriptor = descriptorForIndex(2);
        IndicesAdminClient client = successfulClient();

        AcknowledgedResponse response = execute(descriptor, state(indexMetadata(INDEX_NAME, 1L, null)), client);

        assertTrue(response.isAcknowledged());
        assertRequestTargets(client, INDEX_NAME, descriptor.getMappings());
    }

    public void testDoesNotUpdateCurrentOrNewerMapping() throws Exception {
        for (long installedVersion : new long[] { 2, 3 }) {
            SystemIndexDescriptor descriptor = descriptorForIndex(2);
            IndicesAdminClient client = mock(IndicesAdminClient.class);

            AcknowledgedResponse response = execute(descriptor, state(indexMetadata(INDEX_NAME, installedVersion, null)), client);

            assertTrue(response.isAcknowledged());
            verify(client, never()).putMapping(any(PutMappingRequest.class), any());
        }
    }

    public void testUpdatesWriteIndexBehindAlias() throws Exception {
        SystemIndexDescriptor descriptor = SystemIndexDescriptor.builder(INDEX_PATTERN, "test")
            .setMappingsForWriteAlias(WRITE_ALIAS, mapping(2))
            .build();
        IndicesAdminClient client = successfulClient();

        AcknowledgedResponse response = execute(descriptor, state(indexMetadata(INDEX_NAME, 1L, WRITE_ALIAS)), client);

        assertTrue(response.isAcknowledged());
        assertRequestTargets(client, INDEX_NAME, descriptor.getMappings());
    }

    public void testMissingTargetIsNotAcknowledged() throws Exception {
        SystemIndexDescriptor descriptor = descriptorForIndex(2);
        IndicesAdminClient client = mock(IndicesAdminClient.class);

        AcknowledgedResponse response = execute(descriptor, ClusterState.EMPTY_STATE, client);

        assertFalse(response.isAcknowledged());
        verify(client, never()).putMapping(any(PutMappingRequest.class), any());
    }

    public void testMissingInstalledVersionIsUpdated() throws Exception {
        SystemIndexDescriptor descriptor = descriptorForIndex(1);
        IndicesAdminClient client = successfulClient();
        IndexMetadata metadata = indexMetadata(INDEX_NAME, null, null);

        assertEquals(SystemIndexMappingUpdater.NO_SCHEMA_VERSION, SystemIndexMappingUpdater.getMappingVersion(metadata));
        assertTrue(execute(descriptor, state(metadata), client).isAcknowledged());
        verify(client).putMapping(any(PutMappingRequest.class), any());
    }

    public void testMalformedInstalledVersionFails() {
        SystemIndexDescriptor descriptor = descriptorForIndex(2);
        IndicesAdminClient client = mock(IndicesAdminClient.class);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> execute(descriptor, state(indexMetadata(INDEX_NAME, "one", null)), client)
        );

        assertEquals("system index mapping [_meta.schema_version] must be a number", exception.getMessage());
        verify(client, never()).putMapping(any(PutMappingRequest.class), any());
    }

    public void testDescriptorWithoutMappingsFails() {
        SystemIndexDescriptor descriptor = new SystemIndexDescriptor(INDEX_PATTERN, "test");
        IndicesAdminClient client = mock(IndicesAdminClient.class);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> execute(descriptor, ClusterState.EMPTY_STATE, client)
        );

        assertEquals("system index descriptor does not supply mappings", exception.getMessage());
    }

    private SystemIndexDescriptor descriptorForIndex(long mappingVersion) {
        return SystemIndexDescriptor.builder(INDEX_PATTERN, "test").setMappings(INDEX_NAME, mapping(mappingVersion)).build();
    }

    private IndicesAdminClient successfulClient() {
        IndicesAdminClient client = mock(IndicesAdminClient.class);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = invocation.getArgument(1);
            listener.onResponse(new AcknowledgedResponse(true));
            return null;
        }).when(client).putMapping(any(PutMappingRequest.class), any());
        return client;
    }

    private void assertRequestTargets(IndicesAdminClient client, String index, String mappings) {
        ArgumentCaptor<PutMappingRequest> requestCaptor = ArgumentCaptor.forClass(PutMappingRequest.class);
        verify(client).putMapping(requestCaptor.capture(), any());
        assertThat(requestCaptor.getValue().indices(), arrayContaining(index));
        assertEquals(mappings, requestCaptor.getValue().source());
    }

    private AcknowledgedResponse execute(SystemIndexDescriptor descriptor, ClusterState clusterState, IndicesAdminClient client)
        throws Exception {
        Listener listener = new Listener();
        SystemIndexMappingUpdater.updateMappingIfNecessary(descriptor, clusterState, client, listener);
        if (listener.failure != null) {
            throw listener.failure;
        }
        return listener.response;
    }

    private ClusterState state(IndexMetadata indexMetadata) {
        return ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder().put(indexMetadata, true)).build();
    }

    private IndexMetadata indexMetadata(String name, Object mappingVersion, String writeAlias) {
        IndexMetadata.Builder builder = IndexMetadata.builder(name)
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0);
        Map<String, Object> source = mappingVersion == null
            ? Map.of("properties", Map.of())
            : Map.of("_meta", Map.of(SystemIndexMappingUpdater.SCHEMA_VERSION_META_FIELD, mappingVersion), "properties", Map.of());
        builder.putMapping(new MappingMetadata(MapperService.SINGLE_MAPPING_NAME, source));
        if (writeAlias != null) {
            builder.putAlias(AliasMetadata.builder(writeAlias).writeIndex(true));
        }
        return builder.build();
    }

    private String mapping(long version) {
        return "{\"_meta\":{\"schema_version\":" + version + "},\"properties\":{}}";
    }

    private static class Listener implements ActionListener<AcknowledgedResponse> {
        private AcknowledgedResponse response;
        private Exception failure;

        @Override
        public void onResponse(AcknowledgedResponse response) {
            this.response = response;
        }

        @Override
        public void onFailure(Exception e) {
            this.failure = e;
        }
    }
}
