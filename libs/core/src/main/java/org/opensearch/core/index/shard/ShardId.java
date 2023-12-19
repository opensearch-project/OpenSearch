/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.core.index.shard;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Allows for shard level components to be injected with the shard id.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ShardId implements Comparable<ShardId>, ToXContentFragment, Writeable {

    private final Index index;
    private final int shardId;
    private final int hashCode;

    /**
     * Constructs a new shard id.
     * @param index the index name
     * @param shardId the shard id
     */
    public ShardId(Index index, int shardId) {
        this.index = index;
        this.shardId = shardId;
        this.hashCode = computeHashCode();
    }

    /**
     * Constructs a new shard id with the given index name, index unique identifier, and shard id.
     * @param index the index name
     * @param indexUUID  the index unique identifier
     * @param shardId the shard id
     */
    public ShardId(String index, String indexUUID, int shardId) {
        this(new Index(index, indexUUID), shardId);
    }

    /**
     * Constructs a new shardId from a stream.
     * @param in the stream to read from
     * @throws IOException if an error occurs while reading from the stream
     * @see #writeTo(StreamOutput)
     */
    public ShardId(StreamInput in) throws IOException {
        index = new Index(in);
        shardId = in.readVInt();
        hashCode = computeHashCode();
    }

    /**
     * Writes this shard id to a stream.
     * @param out the stream to write to
     * @throws IOException if an error occurs while writing to the stream
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        index.writeTo(out);
        out.writeVInt(shardId);
    }

    /**
     * Returns the index of this shard id.
     * @return the index of this shard id
     */
    public Index getIndex() {
        return index;
    }

    /**
     * Returns the name of the index of this shard id.
     * @return the name of the index of this shard id
     */
    public String getIndexName() {
        return index.getName();
    }

    /**
     *  Return the shardId of this shard id.
     * @return the shardId of this shard id
     * @see #getId()
     */
    public int id() {
        return this.shardId;
    }

    /**
     * Returns the shard id of this shard id.
     * @return the shard id of this shard id
     */
    public int getId() {
        return id();
    }

    /**
     * Returns a string representation of this shard id.
     * @return "[indexName][shardId]"
     */
    @Override
    public String toString() {
        return "[" + index.getName() + "][" + shardId + "]";
    }

    /**
     * Parse the string representation of this shardId back to an object.
     * <p>
     * We lose index uuid information here, but since we use toString in
     * rest responses, this is the best we can do to reconstruct the object
     * on the client side.
     *
     * @param shardIdString the string representation of the shard id
     *                      (Expect a string of format "[indexName][shardId]" (square brackets included))
     */
    public static ShardId fromString(String shardIdString) {
        int splitPosition = shardIdString.indexOf("][");
        if (splitPosition <= 0 || shardIdString.charAt(0) != '[' || shardIdString.charAt(shardIdString.length() - 1) != ']') {
            throw new IllegalArgumentException("Unexpected shardId string format, expected [indexName][shardId] but got " + shardIdString);
        }
        String indexName = shardIdString.substring(1, splitPosition);
        int shardId = Integer.parseInt(shardIdString.substring(splitPosition + 2, shardIdString.length() - 1));
        return new ShardId(new Index(indexName, Strings.UNKNOWN_UUID_VALUE), shardId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShardId shardId1 = (ShardId) o;
        return shardId == shardId1.shardId && index.equals(shardId1.index);
    }

    /** Returns the hash code of this shard id.
     *
     * @return the hash code of this shard id
     */
    @Override
    public int hashCode() {
        return hashCode;
    }

    /** Computes the hash code of this shard id.
     *
     * @return the hash code of this shard id.
     */
    private int computeHashCode() {
        int result = index != null ? index.hashCode() : 0;
        result = 31 * result + shardId;
        return result;
    }

    /**
     * Compares this ShardId with the specified ShardId.
     * @param o the ShardId to be compared.
     * @return a negative integer, zero, or a positive integer if this ShardId is less than, equal to, or greater than the specified ShardId
     */
    @Override
    public int compareTo(ShardId o) {
        if (o.getId() == shardId) {
            int compare = index.getName().compareTo(o.getIndex().getName());
            if (compare != 0) {
                return compare;
            }
            return index.getUUID().compareTo(o.getIndex().getUUID());
        }
        return Integer.compare(shardId, o.getId());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(toString());
    }
}
