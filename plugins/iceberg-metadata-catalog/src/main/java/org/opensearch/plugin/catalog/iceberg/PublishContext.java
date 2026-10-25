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
import org.apache.iceberg.catalog.TableIdentifier;

import java.io.IOException;
import java.util.Objects;

/**
 * Immutable bundle of per-invocation values used by {@link IcebergMetadataClient#publish}.
 * Loaded once at the top of {@code publish()} and passed to the helpers so they stay
 * decoupled from the catalog lookup.
 */
final class PublishContext {

    private final Table table;
    private final String indexName;
    private final String indexUUID;
    private final int shardId;
    private final String warehousePrefix;

    private PublishContext(Table table, String indexName, String indexUUID, int shardId) {
        this.table = Objects.requireNonNull(table, "table");
        this.indexName = Objects.requireNonNull(indexName, "indexName");
        this.indexUUID = Objects.requireNonNull(indexUUID, "indexUUID");
        if (shardId < 0) {
            throw new IllegalArgumentException("shardId must be non-negative, got " + shardId);
        }
        this.shardId = shardId;
        this.warehousePrefix = WarehousePathResolver.partitionPrefix(indexUUID, shardId);
    }

    /**
     * Loads the Iceberg table from the catalog and reads the required
     * {@code opensearch.index_uuid} property stamped at {@code initialize()} time.
     *
     * @throws IOException if the table does not exist (the orchestrator should have
     *                     called {@code initialize()} first) or if the required property
     *                     is missing (indicates the table was not created by this plugin).
     */
    static PublishContext create(Catalog catalog, TableIdentifier tableId, String indexName, int shardId) throws IOException {
        if (!catalog.tableExists(tableId)) {
            throw new IOException("Iceberg table [" + tableId + "] does not exist; initialize() must be called before publish()");
        }
        Table table = catalog.loadTable(tableId);
        String indexUUID = table.properties().get(IcebergMetadataClient.PROPERTY_INDEX_UUID);
        if (indexUUID == null) {
            throw new IOException(
                "Iceberg table ["
                    + tableId
                    + "] is missing required property ["
                    + IcebergMetadataClient.PROPERTY_INDEX_UUID
                    + "]; table was not created by this plugin"
            );
        }
        return new PublishContext(table, indexName, indexUUID, shardId);
    }

    Table table() {
        return table;
    }

    String indexName() {
        return indexName;
    }

    String indexUUID() {
        return indexUUID;
    }

    int shardId() {
        return shardId;
    }

    /**
     * Returns the warehouse-relative prefix under which this shard's parquet files
     * are written. Equivalent to {@code WarehousePathResolver.partitionPrefix(indexUUID, shardId)}.
     */
    String warehousePrefix() {
        return warehousePrefix;
    }

    /** Builds the absolute S3 URI for a warehouse key. Table location + "/" + key. */
    String absoluteWarehousePath(String warehouseKey) {
        String loc = table.location();
        // S3 URIs don't care about double slashes for correctness, but strip them for hygiene.
        if (loc.endsWith("/")) {
            return loc + warehouseKey;
        }
        return loc + "/" + warehouseKey;
    }
}
