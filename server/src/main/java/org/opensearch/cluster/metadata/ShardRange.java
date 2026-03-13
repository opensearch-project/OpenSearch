/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;

/**
 * Represents the hash range assigned to a shard.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ShardRange implements Comparable<ShardRange>, ToXContentFragment, Writeable {
    private final int shardId;
    private final int start;
    private final int end;

    public ShardRange(int shardId, int start, int end) {
        this.shardId = shardId;
        this.start = start;
        this.end = end;
    }

    /**
     * Constructs a new shard range from a stream.
     * @param in the stream to read from
     * @throws IOException if an error occurs while reading from the stream
     * @see #writeTo(StreamOutput)
     */
    public ShardRange(StreamInput in) throws IOException {
        shardId = in.readVInt();
        start = in.readInt();
        end = in.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ShardRange)) return false;

        ShardRange that = (ShardRange) o;

        if (shardId != that.shardId) return false;
        if (start != that.start) return false;
        return end == that.end;
    }

    @Override
    public int hashCode() {
        int result = shardId;
        result = 31 * result + start;
        result = 31 * result + end;
        return result;
    }

    public int getShardId() {
        return shardId;
    }

    public int getStart() {
        return start;
    }

    public int getEnd() {
        return end;
    }

    public ShardRange copy() {
        return new ShardRange(shardId, start, end);
    }

    public boolean contains(long hash) {
        return hash >= start && hash <= end;
    }

    @Override
    public int compareTo(ShardRange o) {
        return Integer.compare(start, o.start);
    }

    @Override
    public String toString() {
        return "ShardRange{" + "shardId=" + shardId + ", start=" + start + ", end=" + end + '}';
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(shardId);
        out.writeInt(start);
        out.writeInt(end);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().field("shard_id", shardId).field("start", start).field("end", end);
        builder.endObject();
        return builder;
    }

    public static ShardRange parse(XContentParser parser) throws IOException {
        int shardId = -1, start = -1, end = -1;
        XContentParser.Token token;
        String fieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("shard_id".equals(fieldName)) {
                    shardId = parser.intValue();
                } else if ("start".equals(fieldName)) {
                    start = parser.intValue();
                } else if ("end".equals(fieldName)) {
                    end = parser.intValue();
                }
            }
        }

        return new ShardRange(shardId, start, end);
    }
}
