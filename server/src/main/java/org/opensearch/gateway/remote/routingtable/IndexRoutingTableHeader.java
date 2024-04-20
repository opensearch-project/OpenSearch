/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.routingtable;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.opensearch.Version;
import org.opensearch.common.io.stream.BufferedChecksumStreamInput;
import org.opensearch.common.io.stream.BufferedChecksumStreamOutput;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.EOFException;
import java.io.IOException;

/**
 * The stored header information for the individual index routing table
 */
public class IndexRoutingTableHeader {

    private final long routingTableVersion;

    private final String indexName;

    private final Version nodeVersion;

    public static final String INDEX_ROUTING_HEADER_CODEC = "index_routing_header_codec";

    public static final int INITIAL_VERSION = 1;

    public static final int CURRENT_VERSION = INITIAL_VERSION;

    public IndexRoutingTableHeader(long routingTableVersion, String indexName, Version nodeVersion) {
        this.routingTableVersion = routingTableVersion;
        this.indexName = indexName;
        this.nodeVersion = nodeVersion;
    }

    /**
     * Returns the bytes reference for the {@link IndexRoutingTableHeader}
     * @return the {@link BytesReference}
     * @throws IOException
     */
    public BytesReference write() throws IOException {
        BytesReference bytesReference;
        try (
            BytesStreamOutput bytesStreamOutput = new BytesStreamOutput();
            BufferedChecksumStreamOutput out = new BufferedChecksumStreamOutput(bytesStreamOutput)
        ) {
            CodecUtil.writeHeader(new OutputStreamDataOutput(out), INDEX_ROUTING_HEADER_CODEC, CURRENT_VERSION);
            // Write version
            out.writeLong(routingTableVersion);
            out.writeInt(nodeVersion.id);
            out.writeString(indexName);
            // Checksum header
            out.writeInt((int) out.getChecksum());
            out.flush();
            bytesReference = bytesStreamOutput.bytes();
        }
        return bytesReference;
    }

    /**
     * Reads the contents on the byte array into the corresponding {@link IndexRoutingTableHeader}
     * @param inBytes
     * @param source
     * @return
     * @throws IOException
     */
    public IndexRoutingTableHeader read(byte[] inBytes, String source) throws IOException {
        try {
            try (BufferedChecksumStreamInput in = new BufferedChecksumStreamInput(new BytesStreamInput(inBytes), source)) {
                readHeaderVersion(in);
                final int version = in.readInt();
                final int nodeVersion = in.readInt();
                final String name = in.readString();
                verifyChecksum(in);
                assert version >= 0 : "Version must be non-negative [" + version + "]";
                assert in.readByte() == -1 : "Header is not fully read";
                return new IndexRoutingTableHeader(version, name, Version.fromId(nodeVersion));
            }
        } catch (EOFException e) {
            throw new IOException("index routing header truncated", e);
        }
    }

    static void verifyChecksum(BufferedChecksumStreamInput in) throws IOException {
        // This absolutely must come first, or else reading the checksum becomes part of the checksum
        long expectedChecksum = in.getChecksum();
        long readChecksum = Integer.toUnsignedLong(in.readInt());
        if (readChecksum != expectedChecksum) {
            throw new IOException(
                "checksum verification failed - expected: 0x"
                    + Long.toHexString(expectedChecksum)
                    + ", got: 0x"
                    + Long.toHexString(readChecksum)
            );
        }
    }

    static int readHeaderVersion(final StreamInput in) throws IOException {
        final int version;
        try {
            version = CodecUtil.checkHeader(new InputStreamDataInput(in), INDEX_ROUTING_HEADER_CODEC, INITIAL_VERSION, CURRENT_VERSION);
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException e) {
            throw new IOException("index routing table header corrupted", e);
        }
        return version;
    }

    public long getRoutingTableVersion() {
        return routingTableVersion;
    }

    public String getIndexName() {
        return indexName;
    }

    public Version getNodeVersion() {
        return nodeVersion;
    }
}
