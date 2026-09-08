/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.catalog.iceberg;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link PublishContext}. Validates the catalog-lookup contract and
 * absolute-path composition. The resolver's path format itself is covered by
 * {@link WarehousePathResolverTests}.
 */
public class PublishContextTests extends OpenSearchTestCase {

    private static final TableIdentifier TABLE = TableIdentifier.of(Namespace.of(IcebergMetadataClient.NAMESPACE), "my-index");

    public void testCreateReadsIndexUuidFromTableProperty() throws IOException {
        Catalog catalog = mock(Catalog.class);
        Table table = mock(Table.class);
        when(catalog.tableExists(any())).thenReturn(true);
        when(catalog.loadTable(any())).thenReturn(table);
        when(table.properties()).thenReturn(Collections.singletonMap(IcebergMetadataClient.PROPERTY_INDEX_UUID, "uuid-42"));
        when(table.location()).thenReturn("s3://warehouse-bucket/ns/my-index");

        PublishContext ctx = PublishContext.create(catalog, TABLE, "my-index", 3);

        assertEquals("uuid-42", ctx.indexUUID());
        assertEquals("my-index", ctx.indexName());
        assertEquals(3, ctx.shardId());
        assertEquals("data/uuid-42/3/", ctx.warehousePrefix());
    }

    public void testCreateFailsWhenTableMissing() {
        Catalog catalog = mock(Catalog.class);
        when(catalog.tableExists(any())).thenReturn(false);

        IOException e = expectThrows(IOException.class, () -> PublishContext.create(catalog, TABLE, "my-index", 0));
        assertTrue(e.getMessage().contains("initialize()"));
    }

    public void testCreateFailsWhenPropertyMissing() {
        Catalog catalog = mock(Catalog.class);
        Table table = mock(Table.class);
        when(catalog.tableExists(any())).thenReturn(true);
        when(catalog.loadTable(any())).thenReturn(table);
        when(table.properties()).thenReturn(Collections.emptyMap());

        IOException e = expectThrows(IOException.class, () -> PublishContext.create(catalog, TABLE, "my-index", 0));
        assertTrue(e.getMessage().contains(IcebergMetadataClient.PROPERTY_INDEX_UUID));
    }

    public void testAbsoluteWarehousePathNoDoubleSlash() throws IOException {
        PublishContext ctx = contextWithLocation("s3://bucket/ns/tbl");
        assertEquals("s3://bucket/ns/tbl/data/uuid-42/0/_0.cfe__UUID", ctx.absoluteWarehousePath("data/uuid-42/0/_0.cfe__UUID"));
    }

    public void testAbsoluteWarehousePathHandlesTrailingSlash() throws IOException {
        PublishContext ctx = contextWithLocation("s3://bucket/ns/tbl/");
        assertEquals("s3://bucket/ns/tbl/data/uuid-42/0/_0.cfe__UUID", ctx.absoluteWarehousePath("data/uuid-42/0/_0.cfe__UUID"));
    }

    private PublishContext contextWithLocation(String location) throws IOException {
        Catalog catalog = mock(Catalog.class);
        Table table = mock(Table.class);
        when(catalog.tableExists(any())).thenReturn(true);
        when(catalog.loadTable(any())).thenReturn(table);
        when(table.properties()).thenReturn(Collections.singletonMap(IcebergMetadataClient.PROPERTY_INDEX_UUID, "uuid-42"));
        when(table.location()).thenReturn(location);
        return PublishContext.create(catalog, TABLE, "my-index", 0);
    }
}
