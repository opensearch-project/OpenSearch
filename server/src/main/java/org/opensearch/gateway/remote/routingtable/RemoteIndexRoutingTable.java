/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.routingtable;


import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.common.io.Streams;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lucene.store.IndexOutputOutputStream;
import org.opensearch.common.remote.AbstractRemoteWritableBlobEntity;
import org.opensearch.common.remote.BlobPathParameters;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.index.Index;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.repositories.blobstore.ChecksumWritableBlobStoreFormat;


import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.opensearch.cluster.routing.remote.InternalRemoteRoutingTableService.INDEX_ROUTING_METADATA_PREFIX;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;
import static org.opensearch.gateway.remote.model.RemoteRoutingTableBlobStore.INDEX_ROUTING_FILE_PREFIX;

/**
 * Remote store object for IndexRoutingTable
 */
public class RemoteIndexRoutingTable extends AbstractRemoteWritableBlobEntity<IndexRoutingTable> {

    public static final String INDEX_ROUTING_TABLE = "index_routing_table";
    private IndexRoutingTable indexRoutingTable;
    private final Index index;
    private long term;
    private long version;
    public static final ChecksumWritableBlobStoreFormat<IndexRoutingTable> INDEX_ROUTING_TABLE_FORMAT = new ChecksumWritableBlobStoreFormat<>(
        "index-routing-table",
        IndexRoutingTable::readFrom
    );

    public RemoteIndexRoutingTable(
        IndexRoutingTable indexRoutingTable,
        String clusterUUID,
        Compressor compressor,
        long term,
        long version
    ) {
        super(clusterUUID, compressor, null);
        this.index = indexRoutingTable.getIndex();
        this.indexRoutingTable = indexRoutingTable;
        this.term = term;
        this.version = version;
        this.blobFileName = generateBlobFileName();
    }

    /**
     * Reads data from inputStream and creates RemoteIndexRoutingTable object with the {@link IndexRoutingTable}
     * @param blobName name of the blob, which contains the index routing data
     * @param index index for the current routing data
     */
    public RemoteIndexRoutingTable(String blobName, Index index, String clusterUUID, Compressor compressor) {
        super(clusterUUID, compressor, null);
        this.index = index;
        this.term = -1;
        this.version = -1;
        this.blobName = blobName;
    }

    public IndexRoutingTable getIndexRoutingTable() {
        return indexRoutingTable;
    }

    public Index getIndex() {
        return index;
    }

    @Override
    public BlobPathParameters getBlobPathParameters() {
        return new BlobPathParameters(List.of(indexRoutingTable.getIndex().getUUID()), "");
    }

    @Override
    public String getType() {
        return INDEX_ROUTING_TABLE;
    }

    @Override
    public String generateBlobFileName() {
        return String.join(
            DELIMITER,
            INDEX_ROUTING_FILE_PREFIX,
            RemoteStoreUtils.invertLong(term),
            RemoteStoreUtils.invertLong(version),
            RemoteStoreUtils.invertLong(System.currentTimeMillis())
        );
    }

    @Override
    public ClusterMetadataManifest.UploadedMetadata getUploadedMetadata() {
        return new ClusterMetadataManifest.UploadedIndexMetadata(index.getName(), index.getUUID(), blobName, INDEX_ROUTING_METADATA_PREFIX);
    }

    @Override
    public InputStream serialize() throws IOException {
        return INDEX_ROUTING_TABLE_FORMAT.serialize(indexRoutingTable, generateBlobFileName(), getCompressor()).streamInput();
    }

    @Override
    public IndexRoutingTable deserialize(InputStream in) throws IOException {
        return INDEX_ROUTING_TABLE_FORMAT.deserialize(blobName, Streams.readFully(in));
    }
}
