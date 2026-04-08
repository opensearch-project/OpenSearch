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

package org.opensearch.index.cache.bitset;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.indices.IndicesBitsetFilterCache;

/**
 * Per-index view into the node-level {@link IndicesBitsetFilterCache}.
 * Binds a per-index {@link IndicesBitsetFilterCache.Listener} so that callers can obtain
 * a {@link BitSetProducer} without needing to supply a listener at every call site.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class BitsetFilterCache {

    private final IndicesBitsetFilterCache indicesCache;
    private final IndicesBitsetFilterCache.Listener listener;

    public BitsetFilterCache(IndicesBitsetFilterCache indicesCache, IndicesBitsetFilterCache.Listener listener) {
        if (indicesCache == null) {
            throw new IllegalArgumentException("indicesCache must not be null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("listener must not be null");
        }
        this.indicesCache = indicesCache;
        this.listener = listener;
    }

    public BitSetProducer getBitSetProducer(Query query) {
        return indicesCache.getBitSetProducer(query, listener);
    }
}
