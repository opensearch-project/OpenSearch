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

package org.opensearch.index.fielddata;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.Accountable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.index.shard.ShardId;

/**
 * A simple field data cache abstraction on the *index* level.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public interface IndexFieldDataCache {

    <FD extends LeafFieldData, IFD extends IndexFieldData<FD>> FD load(LeafReaderContext context, IFD indexFieldData) throws Exception;

    <FD extends LeafFieldData, IFD extends IndexFieldData.Global<FD>> IFD load(DirectoryReader indexReader, IFD indexFieldData)
        throws Exception;

    /**
     * Clears all the field data stored cached in on this index.
     */
    void clear();

    /**
     * Clears all the field data stored cached in on this index for the specified field name.
     */
    void clear(String fieldName);

    /**
     * The listener interface
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    interface Listener {

        /**
         * Called after the fielddata is loaded during the cache phase
         */
        default void onCache(ShardId shardId, String fieldName, Accountable ramUsage) {}

        /**
         * Called after the fielddata is unloaded
         */
        default void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes) {}

        /**
         * Same as {@link #onCache(ShardId, String, Accountable)} but carries {@code shardIdentity} —
         * the {@code System.identityHashCode} of the IndexShard at cache-load time.
         */
        default void onCache(ShardId shardId, String fieldName, Accountable ramUsage, int shardIdentity) {
            onCache(shardId, fieldName, ramUsage);
        }

        /**
         * Same as {@link #onRemoval(ShardId, String, boolean, long)} but carries the
         * {@code shardIdentity} captured at cache-load time. Per-shard listeners should compare it
         * to the current shard's identity and skip if they differ; this avoids decrementing a
         * replacement shard for an entry it never cached (#20363). Node-wide listeners (e.g. the
         * field-data circuit breaker) should ignore identity and always run.
         */
        default void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes, int shardIdentity) {
            onRemoval(shardId, fieldName, wasEvicted, sizeInBytes);
        }
    }

    /**
     * No index field data cache
     *
     * @opensearch.internal
     */
    class None implements IndexFieldDataCache {

        @Override
        public <FD extends LeafFieldData, IFD extends IndexFieldData<FD>> FD load(LeafReaderContext context, IFD indexFieldData)
            throws Exception {
            return indexFieldData.loadDirect(context);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <FD extends LeafFieldData, IFD extends IndexFieldData.Global<FD>> IFD load(DirectoryReader indexReader, IFD indexFieldData)
            throws Exception {
            return (IFD) indexFieldData.loadGlobalDirect(indexReader);
        }

        @Override
        public void clear() {}

        @Override
        public void clear(String fieldName) {}
    }
}
