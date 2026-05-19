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

import org.apache.lucene.util.Accountable;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.index.AbstractIndexComponent;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Service for field data (cacheing, etc)
 *
 * @opensearch.internal
 */
public class IndexFieldDataService extends AbstractIndexComponent implements Closeable {
    public static final String FIELDDATA_CACHE_VALUE_NODE = "node";
    public static final String FIELDDATA_CACHE_KEY = "index.fielddata.cache";
    public static final Setting<String> INDEX_FIELDDATA_CACHE_KEY = new Setting<>(
        FIELDDATA_CACHE_KEY,
        (s) -> FIELDDATA_CACHE_VALUE_NODE,
        (s) -> {
            switch (s) {
                case "node":
                case "none":
                    return s;
                default:
                    throw new IllegalArgumentException("failed to parse [" + s + "] must be one of [node,none]");
            }
        },
        Property.IndexScope
    );

    private final ThreadPool threadPool;

    private final CircuitBreakerService circuitBreakerService;

    private final IndicesFieldDataCache indicesFieldDataCache;
    // the below map needs to be modified under a lock
    private final Map<String, IndexFieldDataCache> fieldDataCaches = new HashMap<>();
    private final MapperService mapperService;
    private static final IndexFieldDataCache.Listener DEFAULT_NOOP_LISTENER = new IndexFieldDataCache.Listener() {
        @Override
        public void onCache(ShardId shardId, String fieldName, Accountable ramUsage) {}

        @Override
        public void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes) {}
    };
    private volatile IndexFieldDataCache.Listener listener = DEFAULT_NOOP_LISTENER;

    public IndexFieldDataService(
        IndexSettings indexSettings,
        IndicesFieldDataCache indicesFieldDataCache,
        CircuitBreakerService circuitBreakerService,
        MapperService mapperService,
        ThreadPool threadPool
    ) {
        super(indexSettings);
        this.indicesFieldDataCache = indicesFieldDataCache;
        this.circuitBreakerService = circuitBreakerService;
        this.mapperService = mapperService;
        this.threadPool = threadPool;
    }

    public synchronized void clear() {
        // Since IndexFieldDataCache implementation is now tied to a single node-level IndicesFieldDataCache, it's safe to clear using that
        // IndicesFieldDataCache.
        indicesFieldDataCache.clear(index());
        fieldDataCaches.clear();
    }

    public synchronized void clearField(final String fieldName) {
        indicesFieldDataCache.clear(index(), fieldName);
        fieldDataCaches.remove(fieldName);
    }

    /**
     * Returns fielddata for the provided field type, given the provided fully qualified index name, while also making
     * a {@link SearchLookup} supplier available that is required for runtime fields.
     */
    @SuppressWarnings("unchecked")
    public <IFD extends IndexFieldData<?>> IFD getForField(
        MappedFieldType fieldType,
        String fullyQualifiedIndexName,
        Supplier<SearchLookup> searchLookup
    ) {
        final String fieldName = fieldType.name();
        IndexFieldData.Builder builder = fieldType.fielddataBuilder(fullyQualifiedIndexName, searchLookup);

        IndexFieldDataCache cache;
        synchronized (this) {
            cache = fieldDataCaches.get(fieldName);
            if (cache == null) {
                String cacheType = indexSettings.getValue(INDEX_FIELDDATA_CACHE_KEY);
                if (FIELDDATA_CACHE_VALUE_NODE.equals(cacheType)) {
                    cache = indicesFieldDataCache.buildIndexFieldDataCache(listener, index(), fieldName);
                } else if ("none".equals(cacheType)) {
                    cache = new IndexFieldDataCache.None();
                } else {
                    throw new IllegalArgumentException("cache type not supported [" + cacheType + "] for field [" + fieldName + "]");
                }
                fieldDataCaches.put(fieldName, cache);
            }
        }

        return (IFD) builder.build(cache, circuitBreakerService);
    }

    /**
     * Sets a {@link org.opensearch.index.fielddata.IndexFieldDataCache.Listener} passed to each {@link IndexFieldData}
     * creation to capture onCache and onRemoval events. Setting a listener on this method will override any previously
     * set listeners.
     * @throws IllegalStateException if the listener is set more than once
     */
    public void setListener(IndexFieldDataCache.Listener listener) {
        if (listener == null) {
            throw new IllegalArgumentException("listener must not be null");
        }
        if (this.listener != DEFAULT_NOOP_LISTENER) {
            throw new IllegalStateException("can't set listener more than once");
        }
        this.listener = listener;
    }

    @Override
    public void close() throws IOException {
        // Clear the field data cache for this index in an async manner
        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
            try {
                this.clear();
            } catch (Exception ex) {
                logger.warn(
                    "Exception occurred while clearing index field data cache for index: {}, exception: {}",
                    indexSettings.getIndex().getName(),
                    ex
                );
            }
        });
    }
}
