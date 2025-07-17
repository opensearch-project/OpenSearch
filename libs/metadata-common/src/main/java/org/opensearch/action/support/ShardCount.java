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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.action.support;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

/**
 * A base class whose instances represent a value for counting the number
 * of shard copies for a given shard in an index. This class provides the core functionality
 * for shard count validation and management.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.2.0")
public class ShardCount implements Writeable {

    protected static final int ACTIVE_SHARD_COUNT_DEFAULT = -2;
    protected static final int ALL_ACTIVE_SHARDS = -1;

    public static final ShardCount DEFAULT = new ShardCount(ACTIVE_SHARD_COUNT_DEFAULT);
    public static final ShardCount ALL = new ShardCount(ALL_ACTIVE_SHARDS);
    public static final ShardCount NONE = new ShardCount(0);
    public static final ShardCount ONE = new ShardCount(1);

    protected final int value;

    protected ShardCount(final int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    /**
     * Creates a new ShardCount instance for the given value.
     * The value must be non-negative.
     *
     * @param value The number of required shard copies
     * @return A new ShardCount instance
     * @throws IllegalArgumentException if value is negative
     */
    public static ShardCount from(final int value) {
        if (value < 0) {
            throw new IllegalArgumentException("shard count cannot be a negative value");
        }
        return get(value);
    }

    /**
     * Validates that the instance is valid for the given number of replicas in an index.
     */
    public boolean validate(final int numberOfReplicas) {
        assert numberOfReplicas >= 0;
        return value <= numberOfReplicas + 1;
    }

    private static ShardCount get(final int value) {
        switch (value) {
            case ACTIVE_SHARD_COUNT_DEFAULT:
                return DEFAULT;
            case ALL_ACTIVE_SHARDS:
                return ALL;
            case 1:
                return ONE;
            case 0:
                return NONE;
            default:
                assert value > 1;
                return new ShardCount(value);
        }
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeInt(value);
    }

    public static ShardCount readFrom(final StreamInput in) throws IOException {
        return get(in.readInt());
    }

    /**
     * Parses a string representation of shard count requirements.
     * Accepts "all" for all shards, null for default value,
     * or a non-negative integer.
     *
     * @param str The string to parse
     * @return A new ShardCount instance
     * @throws IllegalArgumentException if the string cannot be parsed
     */
    public static ShardCount parseString(final String str) {
        if (str == null) {
            return ShardCount.DEFAULT;
        } else if (str.equals("all")) {
            return ShardCount.ALL;
        } else {
            int val;
            try {
                val = Integer.parseInt(str);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("cannot parse ShardCount[" + str + "]", e);
            }
            return ShardCount.from(val);
        }
    }

    /**
     * Returns true if the given number of active shards is enough to meet
     * the required shard count represented by this instance. This method
     * should only be invoked with {@link ShardCount} objects created
     * from {@link #from(int)}, or {@link #NONE} or {@link #ONE}.
     */
    public boolean enoughShardsActive(final int activeShardCount) {
        if (this.value < 0) {
            throw new IllegalStateException("not enough information to resolve to shard count");
        }
        if (activeShardCount < 0) {
            throw new IllegalArgumentException("activeShardCount cannot be negative");
        }
        return this.value <= activeShardCount;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ShardCount that = (ShardCount) o;
        return value == that.value;
    }

    @Override
    public String toString() {
        switch (value) {
            case ALL_ACTIVE_SHARDS:
                return "ALL";
            case ACTIVE_SHARD_COUNT_DEFAULT:
                return "DEFAULT";
            default:
                return Integer.toString(value);
        }
    }

}
