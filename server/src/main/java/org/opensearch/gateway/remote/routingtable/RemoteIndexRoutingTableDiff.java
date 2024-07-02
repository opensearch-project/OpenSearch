/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.routingtable;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.opensearch.cluster.Diff;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.common.io.Streams;
import org.opensearch.common.remote.AbstractRemoteWritableBlobEntity;
import org.opensearch.common.remote.BlobPathParameters;
import org.opensearch.core.common.io.stream.BufferedChecksumStreamInput;
import org.opensearch.core.common.io.stream.BufferedChecksumStreamOutput;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.compress.Compressor;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.repositories.blobstore.ChecksumWritableBlobStoreFormat;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;

/**
 * Represents a difference between {@link IndexRoutingTable} objects that can be serialized and deserialized.
 * This class is responsible for writing and reading the differences between IndexRoutingTables to and from an input/output stream.
 */
public class RemoteIndexRoutingTableDiff extends AbstractRemoteWritableBlobEntity<RemoteIndexRoutingTableDiff>
    implements
        Diff<IndexRoutingTable>,
        Writeable {

    private final Map<String, Diff<IndexRoutingTable>> diffs;

    private long term;
    private long version;

    public static final String INDEX_ROUTING_TABLE_DIFF = "index-routing-diff";

    public static final String INDEX_ROUTING_DIFF_METADATA_PREFIX = "indexRoutingDiff--";

    public static final String INDEX_ROUTING_DIFF_FILE = "index_routing_diff";
    private static final String codec = "RemoteIndexRoutingTableDiff";
    public static final String INDEX_ROUTING_DIFF_PATH_TOKEN = "index-routing-diff";

    public static final int VERSION = 1;

    public static final ChecksumWritableBlobStoreFormat<RemoteIndexRoutingTableDiff> RemoteIndexRoutingTableDiffFormat =
        new ChecksumWritableBlobStoreFormat<>(codec, RemoteIndexRoutingTableDiff::new);

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
     * Reads data from inputStream and creates RemoteIndexRoutingTableDiff object with the {@link Diff<IndexRoutingTable>}
     *
     * @param inputStream input stream with diff data
     * @throws IOException exception thrown on failing to read from stream.
     */
    public RemoteIndexRoutingTableDiff(InputStream inputStream) throws IOException {
        super();
        try (BufferedChecksumStreamInput in = new BufferedChecksumStreamInput(new InputStreamStreamInput(inputStream), "assertion")) {
            CodecUtil.checkHeader(new InputStreamDataInput(inputStream), codec, VERSION, VERSION);
            int size = in.readVInt();
            Map<String, Diff<IndexRoutingTable>> diffs = new HashMap<>();

            for (int i = 0; i < size; i++) {
                String key = in.readString();
                List<IndexShardRoutingTable> shardRoutingTables = new ArrayList<>();

                // Read each IndexShardRoutingTable from the stream
                int numShards = in.readVInt();
                for (int j = 0; j < numShards; j++) {
                    IndexShardRoutingTable shardRoutingTable = IndexShardRoutingTable.Builder.readFrom(in);
                    shardRoutingTables.add(shardRoutingTable);
                }

                // Create a diff object for the index
                Diff<IndexRoutingTable> diff = new RemoteIndexShardRoutingTableDiff(shardRoutingTables);

                // Put the diff into the map with the key
                diffs.put(key, diff);
            }
            verifyCheckSum(in);
            this.diffs = diffs;
        } catch (EOFException e) {
            throw new IOException("Diffs data is corrupted", e);
        }
    }

    /**
     * Writes {@link Diff<IndexRoutingTable>} to the given stream
     *
     * @param streamOutput output stream to write
     * @throws IOException exception thrown on failing to write to stream.
     */
    @Override
    public void writeTo(StreamOutput streamOutput) throws IOException {
        try (BufferedChecksumStreamOutput out = new BufferedChecksumStreamOutput(streamOutput)) {
            // Write codec header
            CodecUtil.writeHeader(new OutputStreamDataOutput(streamOutput), codec, VERSION);

            out.writeVInt(diffs.size());
            for (Map.Entry<String, Diff<IndexRoutingTable>> entry : diffs.entrySet()) {
                out.writeString(entry.getKey());
                entry.getValue().writeTo(out);
            }

            out.writeLong(out.getChecksum());
            out.flush();
        } catch (IOException e) {
            throw new IOException("Failed to write Diff<IndexRoutingTable> to stream", e);
        }
    }

    /**
     * Applies the differences to the provided {@link IndexRoutingTable}.
     *
     * @param part the initial {@link IndexRoutingTable}.
     * @return the modified {@link IndexRoutingTable} with applied differences.
     */
    @Override
    public IndexRoutingTable apply(IndexRoutingTable part) {
        // Apply diffs to the provided IndexRoutingTable
        for (Map.Entry<String, Diff<IndexRoutingTable>> entry : diffs.entrySet()) {
            part = entry.getValue().apply(part);
        }
        return part;
    }

    /**
     * Verifies the checksum of the data in the given input stream.
     *
     * @param in the input stream containing the data.
     * @throws IOException if the checksum verification fails or if an I/O exception occurs.
     */
    private void verifyCheckSum(BufferedChecksumStreamInput in) throws IOException {
        long expectedChecksum = in.getChecksum();
        long readChecksum = in.readLong();
        if (readChecksum != expectedChecksum) {
            throw new IOException(
                "checksum verification failed - expected: 0x"
                    + Long.toHexString(expectedChecksum)
                    + ", got: 0x"
                    + Long.toHexString(readChecksum)
            );
        }
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
        return RemoteIndexRoutingTableDiffFormat.serialize(this, generateBlobFileName(), getCompressor()).streamInput();
    }

    @Override
    public RemoteIndexRoutingTableDiff deserialize(InputStream in) throws IOException {
        return RemoteIndexRoutingTableDiffFormat.deserialize(blobName, Streams.readFully(in));
    }
}
