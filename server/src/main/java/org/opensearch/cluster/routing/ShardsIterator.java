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

package org.opensearch.cluster.routing;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.util.Countable;

import java.util.List;

/**
 * Allows to iterate over unrelated shards.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public interface ShardsIterator extends Iterable<ShardRouting>, Countable {

    /**
     * Resets the iterator to its initial state.
     */
    void reset();

    /**
     * The number of shard routing instances.
     *
     * @return number of shard routing instances in this iterator
     */
    int size();

    /**
     * The number of active shard routing instances
     *
     * @return number of active shard routing instances
     */
    int sizeActive();

    /**
     * Returns the next shard, or {@code null} if none available.
     */
    ShardRouting nextOrNull();

    /**
     * Return the number of shards remaining in this {@link ShardsIterator}
     *
     * @return number of shard remaining
     */
    int remaining();

    @Override
    int hashCode();

    @Override
    boolean equals(Object other);

    /**
     * Returns the {@link ShardRouting}s that this shards iterator holds.
     */
    List<ShardRouting> getShardRoutings();
}
