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
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.EOFException;
import java.io.IOException;

/**
 * The stored header information for the individual index routing table
 */
public class IndexRoutingTableHeader implements Writeable {

    public static final String INDEX_ROUTING_HEADER_CODEC = "index_routing_header_codec";
    public static final int INITIAL_VERSION = 1;
    public static final int CURRENT_VERSION = INITIAL_VERSION;
    private final String indexName;

    public IndexRoutingTableHeader(String indexName) {
        this.indexName = indexName;
    }

    /**
     * Reads the contents on the stream into the corresponding {@link IndexRoutingTableHeader}
     *
     * @param in streamInput
     * @throws IOException exception thrown on failing to read from stream.
     */
    public IndexRoutingTableHeader(StreamInput in) throws IOException {
        try {
            readHeaderVersion(in);
            indexName = in.readString();
        } catch (EOFException e) {
            throw new IOException("index routing header truncated", e);
        }
    }

    private void readHeaderVersion(final StreamInput in) throws IOException {
        try {
            CodecUtil.checkHeader(new InputStreamDataInput(in), INDEX_ROUTING_HEADER_CODEC, INITIAL_VERSION, CURRENT_VERSION);
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException e) {
            throw new IOException("index routing table header corrupted", e);
        }
    }

    /**
     * Write the IndexRoutingTable to given stream.
     *
     * @param out stream to write
     * @throws IOException exception thrown on failing to write to stream.
     */
    public void writeTo(StreamOutput out) throws IOException {
        try {
            CodecUtil.writeHeader(new OutputStreamDataOutput(out), INDEX_ROUTING_HEADER_CODEC, CURRENT_VERSION);
            out.writeString(indexName);
            out.flush();
        } catch (IOException e) {
            throw new IOException("Failed to write IndexRoutingTable header", e);
        }
    }

    public String getIndexName() {
        return indexName;
    }

}
