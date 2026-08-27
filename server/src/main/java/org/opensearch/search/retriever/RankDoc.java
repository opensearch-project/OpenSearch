/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * A single entry in a resolved retriever ranking: the document identified by its
 * {@code (index, shardId, _id)} coordinate, together with the pre-computed relevance
 * {@code score} and the retriever-assigned {@code position} (rank).
 * <p>
 * The coordinator produces a list of these after resolving the retriever tree (fusion, reranking,
 * pinning, etc.) and ships it to every shard as part of the internal {@link RankDocsQuery}. {@code _id}
 * is unique only within an index and a document routes to exactly one shard, so the
 * {@code (index, shardId)} pair disambiguates a cross-index {@code _id} collision on the broadcast query.
 *
 * @opensearch.internal
 */
public final class RankDoc implements Writeable, ToXContentObject {

    static final String INDEX_FIELD = "index";
    static final String SHARD_FIELD = "shard";
    static final String ID_FIELD = "id";
    static final String SCORE_FIELD = "score";
    static final String POSITION_FIELD = "position";

    private final String index;
    private final int shardId;
    private final String id;
    private final float score;
    private final int position;

    /**
     * @param index    the concrete index name the document belongs to
     * @param shardId  the shard the document routes to within {@code index}
     * @param id       the document {@code _id} (original string form, not the encoded {@link org.opensearch.index.mapper.Uid} bytes)
     * @param score    the pre-computed retriever score for the document
     * @param position the retriever-assigned rank (0-based); lower means higher in the ranking
     */
    public RankDoc(String index, int shardId, String id, float score, int position) {
        this.index = Objects.requireNonNull(index, "index");
        this.shardId = shardId;
        this.id = Objects.requireNonNull(id, "id");
        this.score = score;
        this.position = position;
    }

    /** Reads a {@code RankDoc} from the wire. Field order must match {@link #writeTo}. */
    public RankDoc(StreamInput in) throws IOException {
        this.index = in.readString();
        this.shardId = in.readVInt();
        this.id = in.readString();
        this.score = in.readFloat();
        this.position = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeVInt(shardId);
        out.writeString(id);
        out.writeFloat(score);
        out.writeVInt(position);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INDEX_FIELD, index);
        builder.field(SHARD_FIELD, shardId);
        builder.field(ID_FIELD, id);
        builder.field(SCORE_FIELD, score);
        builder.field(POSITION_FIELD, position);
        builder.endObject();
        return builder;
    }

    /** The concrete index name the document belongs to. */
    public String index() {
        return index;
    }

    /** The shard the document routes to within its index. */
    public int shardId() {
        return shardId;
    }

    /** The document {@code _id} in its original string form. */
    public String id() {
        return id;
    }

    /** The pre-computed retriever score for the document. */
    public float score() {
        return score;
    }

    /** The retriever-assigned rank (0-based); lower means higher in the ranking. */
    public int position() {
        return position;
    }

    /** Whether this doc belongs to the given {@code (index, shardId)} shard. */
    public boolean belongsTo(String indexName, int shard) {
        return this.shardId == shard && this.index.equals(indexName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RankDoc other = (RankDoc) o;
        return shardId == other.shardId
            && position == other.position
            && Float.compare(other.score, score) == 0
            && index.equals(other.index)
            && id.equals(other.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, shardId, id, score, position);
    }

    @Override
    public String toString() {
        return "RankDoc{index='" + index + "', shardId=" + shardId + ", id='" + id + "', score=" + score + ", position=" + position + '}';
    }
}
