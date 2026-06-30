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

package org.opensearch.indices.analysis;

import org.opensearch.OpenSearchException;
import org.opensearch.Version;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A factory for the pre-built cache
 *
 * @opensearch.internal
 */
public class PreBuiltCacheFactory {

    /**
     * The strategy of caching the analyzer
     * <ul>
     * <li>ONE        : Exactly one version is stored. Useful for analyzers which do not store version information</li>
     * <li>LUCENE     : Exactly one version for each lucene version is stored. Useful to prevent different analyzers with the same version</li>
     * <li>OPENSEARCH : Exactly one version per opensearch version is stored. Useful if you change an analyzer between opensearch releases, when the lucene version does not change</li>
     * </ul>
     */
    public enum CachingStrategy {
        ONE,
        LUCENE,
        OPENSEARCH
    }

    /**
     * The prebuilt cache
     *
     * @opensearch.internal
     */
    public interface PreBuiltCache<T> {

        T get(Version version);

        void put(Version version, T t);

        Collection<T> values();
    }

    private PreBuiltCacheFactory() {}

    public static <T> PreBuiltCache<T> getCache(CachingStrategy cachingStrategy) {
        switch (cachingStrategy) {
            case ONE:
                return new PreBuiltCacheStrategyOne<>();
            case LUCENE:
                return new PreBuiltCacheStrategyLucene<>();
            case OPENSEARCH:
                return new PreBuiltCacheStrategyOpenSearch<>();
            default:
                throw new OpenSearchException("No action configured for caching strategy[" + cachingStrategy + "]");
        }
    }

    /**
     * This is a pretty simple cache, it only contains one version
     *
     * @opensearch.internal
     */
    private static class PreBuiltCacheStrategyOne<T> implements PreBuiltCache<T> {

        private T model = null;

        @Override
        public T get(Version version) {
            return model;
        }

        @Override
        public void put(Version version, T model) {
            this.model = model;
        }

        @Override
        public Collection<T> values() {
            return model == null ? Collections.emptySet() : Collections.singleton(model);
        }
    }

    /**
     * This cache contains one version for each opensearch version object
     *
     * @opensearch.internal
     */
    private static class PreBuiltCacheStrategyOpenSearch<T> implements PreBuiltCache<T> {

        Map<Version, T> mapModel = new HashMap<>(2);

        @Override
        public T get(Version version) {
            return mapModel.get(version);
        }

        @Override
        public void put(Version version, T model) {
            mapModel.put(version, model);
        }

        @Override
        public Collection<T> values() {
            return mapModel.values();
        }
    }

    /**
     * This cache uses the lucene version for caching
     *
     * @opensearch.internal
     */
    private static class PreBuiltCacheStrategyLucene<T> implements PreBuiltCache<T> {

        private Map<org.apache.lucene.util.Version, T> mapModel = new HashMap<>(2);

        @Override
        public T get(Version version) {
            return mapModel.get(version.luceneVersion);
        }

        @Override
        public void put(org.opensearch.Version version, T model) {
            mapModel.put(version.luceneVersion, model);
        }

        @Override
        public Collection<T> values() {
            return mapModel.values();
        }
    }
}
