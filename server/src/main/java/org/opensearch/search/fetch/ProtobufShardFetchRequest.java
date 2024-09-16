/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.fetch;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.opensearch.action.search.ProtobufSearchShardTask;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.search.RescoreDocIds;
import org.opensearch.search.dfs.AggregatedDfs;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.internal.ProtobufShardSearchRequest;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.tasks.ProtobufTask;
import org.opensearch.tasks.ProtobufTaskId;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Map;

/**
 * Shard level fetch base request. Holds all the info needed to execute a fetch.
 * Used with search scroll as the original request doesn't hold indices.
 *
 * @opensearch.internal
 */
public class ProtobufShardFetchRequest extends TransportRequest {

    // TODO: proto message
    private ShardSearchContextId contextId;

    private int[] docIds;

    private int size;

    private ScoreDoc lastEmittedDoc;

    public ProtobufShardFetchRequest(ShardSearchContextId contextId, Collection<Integer> list, ScoreDoc lastEmittedDoc) {
        this.contextId = contextId;
        this.docIds = list.stream().mapToInt(Integer::intValue).toArray();
        this.size = list.size();
        this.lastEmittedDoc = lastEmittedDoc;
    }

    public ProtobufShardFetchRequest(byte[] in) throws IOException {
        super(in);
        // contextId = new ShardSearchContextId(in);
        // size = in.readVInt();
        // docIds = new int[size];
        // for (int i = 0; i < size; i++) {
        //     docIds[i] = in.readVInt();
        // }
        // byte flag = in.readByte();
        // if (flag == 1) {
        //     lastEmittedDoc = Lucene.readFieldDoc(in);
        // } else if (flag == 2) {
        //     lastEmittedDoc = Lucene.readScoreDoc(in);
        // } else if (flag != 0) {
        //     throw new IOException("Unknown flag: " + flag);
        // }
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
        super.writeTo(out);
        // contextId.writeTo(out);
        // out.writeVInt(size);
        // for (int i = 0; i < size; i++) {
        //     out.writeVInt(docIds[i]);
        // }
        // if (lastEmittedDoc == null) {
        //     out.writeByte((byte) 0);
        // } else if (lastEmittedDoc instanceof FieldDoc) {
        //     out.writeByte((byte) 1);
        //     Lucene.writeFieldDoc(out, (FieldDoc) lastEmittedDoc);
        // } else {
        //     out.writeByte((byte) 2);
        //     Lucene.writeScoreDoc(out, lastEmittedDoc);
        // }
    }

    public ShardSearchContextId contextId() {
        return contextId;
    }

    public int[] docIds() {
        return docIds;
    }

    public int docIdsSize() {
        return size;
    }

    public ScoreDoc lastEmittedDoc() {
        return lastEmittedDoc;
    }

    @Override
    public ProtobufTask createProtobufTask(long id, String type, String action, ProtobufTaskId parentTaskId, Map<String, String> headers) {
        return new ProtobufSearchShardTask(id, type, action, getDescription(), parentTaskId, headers);
    }

    @Override
    public String getDescription() {
        return "id[" + contextId + "], size[" + size + "], lastEmittedDoc[" + lastEmittedDoc + "]";
    }

    @Nullable
    public ShardSearchRequest getShardSearchRequest() {
        return null;
    }

    @Nullable
    public ProtobufShardSearchRequest getProtobufShardSearchRequest() {
        return null;
    }

    @Nullable
    public RescoreDocIds getRescoreDocIds() {
        return null;
    }

    @Nullable
    public AggregatedDfs getAggregatedDfs() {
        return null;
    }
}
