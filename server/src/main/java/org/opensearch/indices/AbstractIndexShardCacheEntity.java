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

package org.opensearch.indices;

import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.index.cache.request.ShardRequestCache;
import org.opensearch.index.shard.IndexShard;

/**
 * Abstract base class for the an {@link IndexShard} level {@linkplain IndicesRequestCache.CacheEntity}.
 *
 * @opensearch.internal
 */
abstract class AbstractIndexShardCacheEntity implements IndicesRequestCache.CacheEntity {

    /**
     * Get the {@linkplain ShardRequestCache} used to track cache statistics.
     */
    protected abstract ShardRequestCache stats();

    @Override
    public final void onCached(IndicesRequestCache.Key key, BytesReference value) {
        stats().onCached(key, value);
    }

    @Override
    public final void onHit() {
        stats().onHit();
    }

    @Override
    public final void onMiss() {
        stats().onMiss();
    }

    @Override
    public final void onRemoval(RemovalNotification<IndicesRequestCache.Key, BytesReference> notification) {
        stats().onRemoval(notification.getKey(), notification.getValue(), notification.getRemovalReason() == RemovalReason.EVICTED);
    }
}
