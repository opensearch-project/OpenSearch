/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.routingtable;

import org.opensearch.cluster.Diff;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTableIncrementalDiff;
import org.opensearch.common.io.Streams;
import org.opensearch.common.remote.AbstractClusterMetadataWriteableBlobEntity;
import org.opensearch.common.remote.BlobPathParameters;
import org.opensearch.core.compress.Compressor;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.repositories.blobstore.ChecksumWritableBlobStoreFormat;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;

/**
 * Represents a incremental difference between {@link org.opensearch.cluster.routing.RoutingTable} objects that can be serialized and deserialized.
 * This class is responsible for writing and reading the differences between RoutingTables to and from an input/output stream.
 */
public class RemoteRoutingTableDiff extends AbstractClusterMetadataWriteableBlobEntity<RoutingTableIncrementalDiff> {
    private final RoutingTableIncrementalDiff routingTableIncrementalDiff;

    private long term;
    private long version;

    public static final String ROUTING_TABLE_DIFF = "routing-table-diff";

    public static final String ROUTING_TABLE_DIFF_METADATA_PREFIX = "routingTableDiff--";

    public static final String ROUTING_TABLE_DIFF_FILE = "routing_table_diff";
    private static final String codec = "RemoteRoutingTableDiff";
    public static final String ROUTING_TABLE_DIFF_PATH_TOKEN = "routing-table-diff";

    public static final int VERSION = 1;

    public static final ChecksumWritableBlobStoreFormat<RoutingTableIncrementalDiff> REMOTE_ROUTING_TABLE_DIFF_FORMAT =
        new ChecksumWritableBlobStoreFormat<>(codec, RoutingTableIncrementalDiff::readFrom);

    /**
     * Constructs a new RemoteRoutingTableDiff with the given differences.
     *
     * @param routingTableIncrementalDiff a RoutingTableIncrementalDiff object containing the differences of {@link IndexRoutingTable}.
     * @param clusterUUID the cluster UUID.
     * @param compressor the compressor to be used.
     * @param term the term of the routing table.
     * @param version the version of the routing table.
     */
    public RemoteRoutingTableDiff(
        RoutingTableIncrementalDiff routingTableIncrementalDiff,
        String clusterUUID,
        Compressor compressor,
        long term,
        long version
    ) {
        super(clusterUUID, compressor);
        this.routingTableIncrementalDiff = routingTableIncrementalDiff;
        this.term = term;
        this.version = version;
    }

    /**
     * Constructs a new RemoteRoutingTableDiff with the given differences.
     *
     * @param routingTableIncrementalDiff a RoutingTableIncrementalDiff object containing the differences of {@link IndexRoutingTable}.
     * @param clusterUUID the cluster UUID.
     * @param compressor the compressor to be used.
     */
    public RemoteRoutingTableDiff(RoutingTableIncrementalDiff routingTableIncrementalDiff, String clusterUUID, Compressor compressor) {
        super(clusterUUID, compressor);
        this.routingTableIncrementalDiff = routingTableIncrementalDiff;
    }

    /**
     * Constructs a new RemoteIndexRoutingTableDiff with the given blob name, cluster UUID, and compressor.
     *
     * @param blobName the name of the blob.
     * @param clusterUUID the cluster UUID.
     * @param compressor the compressor to be used.
     */
    public RemoteRoutingTableDiff(String blobName, String clusterUUID, Compressor compressor) {
        super(clusterUUID, compressor);
        this.routingTableIncrementalDiff = null;
        this.blobName = blobName;
    }

    /**
     * Gets the map of differences of {@link IndexRoutingTable}.
     *
     * @return a map containing the differences.
     */
    public Map<String, Diff<IndexRoutingTable>> getDiffs() {
        assert routingTableIncrementalDiff != null;
        return routingTableIncrementalDiff.getDiffs();
    }

    @Override
    public BlobPathParameters getBlobPathParameters() {
        return new BlobPathParameters(List.of(ROUTING_TABLE_DIFF_PATH_TOKEN), ROUTING_TABLE_DIFF_METADATA_PREFIX);
    }

    @Override
    public String getType() {
        return ROUTING_TABLE_DIFF;
    }

    @Override
    public String generateBlobFileName() {
        if (blobFileName == null) {
            blobFileName = String.join(
                DELIMITER,
                getBlobPathParameters().getFilePrefix(),
                RemoteStoreUtils.invertLong(term),
                RemoteStoreUtils.invertLong(version),
                RemoteStoreUtils.invertLong(System.currentTimeMillis())
            );
        }
        return blobFileName;
    }

    @Override
    public ClusterMetadataManifest.UploadedMetadata getUploadedMetadata() {
        assert blobName != null;
        return new ClusterMetadataManifest.UploadedMetadataAttribute(ROUTING_TABLE_DIFF_FILE, blobName);
    }

    @Override
    public InputStream serialize() throws IOException {
        assert routingTableIncrementalDiff != null;
        return REMOTE_ROUTING_TABLE_DIFF_FORMAT.serialize(routingTableIncrementalDiff, generateBlobFileName(), getCompressor())
            .streamInput();
    }

    @Override
    public RoutingTableIncrementalDiff deserialize(InputStream in) throws IOException {
        return REMOTE_ROUTING_TABLE_DIFF_FORMAT.deserialize(blobName, Streams.readFully(in));
    }
}
