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
import org.opensearch.common.remote.AbstractRemoteWritableBlobEntity;
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
 * Represents a difference between {@link IndexRoutingTable} objects that can be serialized and deserialized.
 * This class is responsible for writing and reading the differences between IndexRoutingTables to and from an input/output stream.
 */
public class RemoteIndexRoutingTableDiff extends AbstractRemoteWritableBlobEntity<RoutingTableIncrementalDiff> {
    private final Map<String, Diff<IndexRoutingTable>> diffs;

    private long term;
    private long version;

    public static final String INDEX_ROUTING_TABLE_DIFF = "index-routing-diff";

    public static final String INDEX_ROUTING_DIFF_METADATA_PREFIX = "indexRoutingDiff--";

    public static final String INDEX_ROUTING_DIFF_FILE = "index_routing_diff";
    private static final String codec = "RemoteIndexRoutingTableDiff";
    public static final String INDEX_ROUTING_DIFF_PATH_TOKEN = "index-routing-diff";

    public static final int VERSION = 1;

    public static final ChecksumWritableBlobStoreFormat<RoutingTableIncrementalDiff> RemoteIndexRoutingTableDiffFormat =
        new ChecksumWritableBlobStoreFormat<>(codec, RoutingTableIncrementalDiff::readFrom);

    /**
     * Constructs a new RemoteIndexRoutingTableDiff with the given differences.
     *
     * @param diffs a map containing the differences of {@link IndexRoutingTable}.
     * @param clusterUUID the cluster UUID.
     * @param compressor the compressor to be used.
     * @param term the term of the routing table.
     * @param version the version of the routing table.
     */
    public RemoteIndexRoutingTableDiff(
        Map<String, Diff<IndexRoutingTable>> diffs,
        String clusterUUID,
        Compressor compressor,
        long term,
        long version
    ) {
        super(clusterUUID, compressor);
        this.diffs = diffs;
        this.term = term;
        this.version = version;
    }

    /**
     * Constructs a new RemoteIndexRoutingTableDiff with the given differences.
     *
     * @param diffs a map containing the differences of {@link IndexRoutingTable}.
     * @param clusterUUID the cluster UUID.
     * @param compressor the compressor to be used.
     */
    public RemoteIndexRoutingTableDiff(Map<String, Diff<IndexRoutingTable>> diffs, String clusterUUID, Compressor compressor) {
        super(clusterUUID, compressor);
        this.diffs = diffs;
    }

    /**
     * Constructs a new RemoteIndexRoutingTableDiff with the given blob name, cluster UUID, and compressor.
     *
     * @param blobName the name of the blob.
     * @param clusterUUID the cluster UUID.
     * @param compressor the compressor to be used.
     */
    public RemoteIndexRoutingTableDiff(String blobName, String clusterUUID, Compressor compressor) {
        super(clusterUUID, compressor);
        this.diffs = null;
        this.blobName = blobName;
    }

    /**
     * Gets the map of differences of {@link IndexRoutingTable}.
     *
     * @return a map containing the differences.
     */
    public Map<String, Diff<IndexRoutingTable>> getDiffs() {
        return diffs;
    }

    @Override
    public BlobPathParameters getBlobPathParameters() {
        return new BlobPathParameters(List.of(INDEX_ROUTING_DIFF_PATH_TOKEN), INDEX_ROUTING_DIFF_METADATA_PREFIX);
    }

    @Override
    public String getType() {
        return INDEX_ROUTING_TABLE_DIFF;
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
        return new ClusterMetadataManifest.UploadedMetadataAttribute(INDEX_ROUTING_DIFF_FILE, blobName);
    }

    @Override
    public InputStream serialize() throws IOException {
        return RemoteIndexRoutingTableDiffFormat.serialize(new RoutingTableIncrementalDiff(diffs), generateBlobFileName(), getCompressor())
            .streamInput();
    }

    @Override
    public RoutingTableIncrementalDiff deserialize(InputStream in) throws IOException {
        return RemoteIndexRoutingTableDiffFormat.deserialize(blobName, Streams.readFully(in));
    }
}
