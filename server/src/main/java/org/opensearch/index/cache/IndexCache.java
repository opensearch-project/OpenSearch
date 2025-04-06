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

package org.opensearch.index.cache;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.AbstractIndexComponent;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.cache.bitset.BitsetFilterCache;
import org.opensearch.index.cache.query.QueryCache;

import java.io.Closeable;
import java.io.IOException;

/**
 * Wrapping class for the index cache
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class IndexCache extends AbstractIndexComponent implements Closeable {

    private final QueryCache queryCache;
    private final BitsetFilterCache bitsetFilterCache;
    private final ClusterIdBoundsCache clusterIdBoundsCache;

    public IndexCache(IndexSettings indexSettings, QueryCache queryCache, BitsetFilterCache bitsetFilterCache, ClusterIdBoundsCache clusterIdBoundsCache) {
        super(indexSettings);
        this.queryCache = queryCache;
        this.bitsetFilterCache = bitsetFilterCache;
        this.clusterIdBoundsCache = clusterIdBoundsCache;
    }

    public QueryCache query() {
        return queryCache;
    }

    /**
     * Return the {@link BitsetFilterCache} for this index.
     */
    public BitsetFilterCache bitsetFilterCache() {
        return bitsetFilterCache;
    }

    public ClusterIdBoundsCache clusterIdBoundsCache() {
        return clusterIdBoundsCache;
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(queryCache, bitsetFilterCache);
    }

    public void clear(String reason) {
        queryCache.clear(reason);
        bitsetFilterCache.clear(reason);
    }

}
