/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.catalog.iceberg;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link IcebergMetadataClient#initialize} focusing on the
 * {@code opensearch.index_uuid} Iceberg table property contract (PR 8 task 8.1):
 * <ul>
 *   <li>Creates the property on first-create with the caller's index UUID.</li>
 *   <li>Accepts an existing table whose stored UUID matches.</li>
 *   <li>Rejects an existing table whose stored UUID differs (namespace collision).</li>
 *   <li>Rejects an existing table missing the property (foreign/legacy table).</li>
 * </ul>
 * Schema inference is covered by {@link OpenSearchSchemaInferenceTests}; full
 * round-trip catalog operations are covered by integration tests (PR 9).
 */
public class IcebergMetadataClientTests extends OpenSearchTestCase {

    private static final String INDEX_NAME = "my-index";
    private static final String INDEX_UUID = "uuid-abc";

    public void testInitializeStampsIndexUuidOnCreate() throws IOException {
        Catalog catalog = mock(Catalog.class);
        Table createdTable = mock(Table.class);
        when(catalog.tableExists(any())).thenReturn(false);
        when(catalog.createTable(any(), any(Schema.class), any(PartitionSpec.class), any())).thenReturn(createdTable);
        when(createdTable.currentSnapshot()).thenReturn(null);

        IcebergMetadataClient client = new IcebergMetadataClient(catalog);
        String snapshotId = client.initialize(INDEX_NAME, indexMetadata(INDEX_UUID));

        assertNull("no snapshot on newly-created table", snapshotId);

        // Verify the property was passed at create time. Capturing via Mockito argument captor
        // would be more fluent, but a simple re-invocation match keeps the test short.
        Map<String, String> expectedProperties = Collections.singletonMap(IcebergMetadataClient.PROPERTY_INDEX_UUID, INDEX_UUID);
        verify(catalog).createTable(
            eq(TableIdentifier.of(org.apache.iceberg.catalog.Namespace.of(IcebergMetadataClient.NAMESPACE), INDEX_NAME)),
            any(Schema.class),
            any(PartitionSpec.class),
            eq(expectedProperties)
        );
    }

    public void testInitializeReusesTableWithMatchingUuid() throws IOException {
        Catalog catalog = mock(Catalog.class);
        Table existingTable = mock(Table.class);
        Map<String, String> storedProperties = new HashMap<>();
        storedProperties.put(IcebergMetadataClient.PROPERTY_INDEX_UUID, INDEX_UUID);

        when(catalog.tableExists(any())).thenReturn(true);
        when(catalog.loadTable(any())).thenReturn(existingTable);
        when(existingTable.properties()).thenReturn(storedProperties);
        when(existingTable.currentSnapshot()).thenReturn(null);

        IcebergMetadataClient client = new IcebergMetadataClient(catalog);
        String snapshotId = client.initialize(INDEX_NAME, indexMetadata(INDEX_UUID));

        assertNull(snapshotId);
        verify(catalog, org.mockito.Mockito.never()).createTable(any(), any(Schema.class), any(PartitionSpec.class), any());
    }

    public void testInitializeRejectsMismatchedUuid() throws IOException {
        Catalog catalog = mock(Catalog.class);
        Table existingTable = mock(Table.class);
        Map<String, String> storedProperties = new HashMap<>();
        storedProperties.put(IcebergMetadataClient.PROPERTY_INDEX_UUID, "uuid-foreign");

        when(catalog.tableExists(any())).thenReturn(true);
        when(catalog.loadTable(any())).thenReturn(existingTable);
        when(existingTable.properties()).thenReturn(storedProperties);

        IcebergMetadataClient client = new IcebergMetadataClient(catalog);
        IOException e = expectThrows(IOException.class, () -> client.initialize(INDEX_NAME, indexMetadata(INDEX_UUID)));
        assertTrue("error message mentions stored UUID", e.getMessage().contains("uuid-foreign"));
        assertTrue("error message mentions current UUID", e.getMessage().contains(INDEX_UUID));
    }

    public void testInitializeRejectsExistingTableMissingProperty() throws IOException {
        Catalog catalog = mock(Catalog.class);
        Table existingTable = mock(Table.class);
        when(catalog.tableExists(any())).thenReturn(true);
        when(catalog.loadTable(any())).thenReturn(existingTable);
        when(existingTable.properties()).thenReturn(Collections.emptyMap());

        IcebergMetadataClient client = new IcebergMetadataClient(catalog);
        IOException e = expectThrows(IOException.class, () -> client.initialize(INDEX_NAME, indexMetadata(INDEX_UUID)));
        assertTrue("error message mentions the property name", e.getMessage().contains(IcebergMetadataClient.PROPERTY_INDEX_UUID));
    }

    private static IndexMetadata indexMetadata(String indexUuid) {
        return minimalIndexMetadata(INDEX_NAME, indexUuid);
    }

    /**
     * Package-private test helper for building a minimal {@link IndexMetadata} keyed on
     * an index name and UUID. Reused by other tests in this package
     * (e.g. {@code ParquetFileUploaderTests}) that need a seed {@link IndexMetadata}
     * to drive schema inference.
     */
    static IndexMetadata minimalIndexMetadata(String indexName, String indexUuid) {
        return IndexMetadata.builder(indexName)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, indexUuid)
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }
}
