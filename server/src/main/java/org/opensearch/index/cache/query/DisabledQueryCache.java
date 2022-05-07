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
import org.opensearch.index.AbstractIndexComponent;
import org.opensearch.index.IndexSettings;

/**
 * NoOp Implementation for a Disabled query cache
 *
 * @opensearch.internal
 */
public class DisabledQueryCache extends AbstractIndexComponent implements QueryCache {

    public DisabledQueryCache(IndexSettings indexSettings) {
        super(indexSettings);
        logger.debug("Using no query cache");
    }

    @Override
    public void close() {
        // nothing to do here
    }

    @Override
    public Weight doCache(Weight weight, QueryCachingPolicy policy) {
        return weight;
    }

    @Override
    public void clear(String reason) {
        // nothing to do here
    }
}
