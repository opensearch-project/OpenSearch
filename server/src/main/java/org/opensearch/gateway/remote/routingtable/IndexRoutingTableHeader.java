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

import java.io.EOFException;
import java.io.IOException;

/**
 * The stored header information for the individual index routing table
 */
public class IndexRoutingTableHeader {

    public static final String INDEX_ROUTING_HEADER_CODEC = "index_routing_header_codec";
    public static final int INITIAL_VERSION = 1;
    public static final int CURRENT_VERSION = INITIAL_VERSION;
    private final String indexName;

    public IndexRoutingTableHeader(String indexName) {
        this.indexName = indexName;
    }

    /**
     * Reads the contents on the byte array into the corresponding {@link IndexRoutingTableHeader}
     *
     * @param in
     * @return IndexRoutingTableHeader
     * @throws IOException
     */
    public static IndexRoutingTableHeader read(StreamInput in) throws IOException {
        try {
            readHeaderVersion(in);
            final String name = in.readString();
            return new IndexRoutingTableHeader(name);
        } catch (EOFException e) {
            throw new IOException("index routing header truncated", e);
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

    /**
     * Returns the bytes reference for the {@link IndexRoutingTableHeader}
     *
     * @throws IOException
     */
    public void write(StreamOutput out) throws IOException {
        CodecUtil.writeHeader(new OutputStreamDataOutput(out), INDEX_ROUTING_HEADER_CODEC, CURRENT_VERSION);
        out.writeString(indexName);
        out.flush();
    }

    public String getIndexName() {
        return indexName;
    }

}
