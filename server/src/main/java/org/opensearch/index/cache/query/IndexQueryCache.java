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

package org.opensearch.index.cache.query;

import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.Weight;
import org.opensearch.OpenSearchException;
import org.opensearch.index.AbstractIndexComponent;
import org.opensearch.index.IndexSettings;
import org.opensearch.indices.IndicesQueryCache;

/**
 * The index-level query cache. This class mostly delegates to the node-level
 * query cache: {@link IndicesQueryCache}.
 *
 * @opensearch.internal
 */
public class IndexQueryCache extends AbstractIndexComponent implements QueryCache {

    final IndicesQueryCache indicesQueryCache;

    public IndexQueryCache(IndexSettings indexSettings, IndicesQueryCache indicesQueryCache) {
        super(indexSettings);
        this.indicesQueryCache = indicesQueryCache;
    }

    @Override
    public void close() throws OpenSearchException {
        clear("close");
    }

    @Override
    public void clear(String reason) {
        logger.debug("full cache clear, reason [{}]", reason);
        indicesQueryCache.clearIndex(index().getName());
    }

    @Override
    public Weight doCache(Weight weight, QueryCachingPolicy policy) {
        return indicesQueryCache.doCache(weight, policy);
    }

}
