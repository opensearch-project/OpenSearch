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

package org.opensearch.ingest.useragent;

import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.ingest.useragent.UserAgentParser.Details;

import java.util.Objects;

class UserAgentCache {
    private final Cache<CompositeCacheKey, Details> cache;

    UserAgentCache(long cacheSize) {
        cache = CacheBuilder.<CompositeCacheKey, Details>builder().setMaximumWeight(cacheSize).build();
    }

    public Details get(String parserName, String userAgent) {
        return cache.get(new CompositeCacheKey(parserName, userAgent));
    }

    public void put(String parserName, String userAgent, Details details) {
        cache.put(new CompositeCacheKey(parserName, userAgent), details);
    }

    private static final class CompositeCacheKey {
        private final String parserName;
        private final String userAgent;

        CompositeCacheKey(String parserName, String userAgent) {
            this.parserName = parserName;
            this.userAgent = userAgent;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof CompositeCacheKey s) {
                return parserName.equals(s.parserName) && userAgent.equals(s.userAgent);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hash(parserName, userAgent);
        }
    }
}
