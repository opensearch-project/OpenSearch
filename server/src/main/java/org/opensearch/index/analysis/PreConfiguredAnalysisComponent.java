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

package org.opensearch.index.analysis;

import org.opensearch.Version;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.indices.analysis.AnalysisModule;
import org.opensearch.indices.analysis.PreBuiltCacheFactory;

import java.io.IOException;

/**
 * Shared implementation for pre-configured analysis components.
 *
 * @opensearch.internal
 */
public abstract class PreConfiguredAnalysisComponent<T> implements AnalysisModule.AnalysisProvider<T> {
    protected final String name;
    protected final PreBuiltCacheFactory.PreBuiltCache<T> cache;

    protected PreConfiguredAnalysisComponent(String name, PreBuiltCacheFactory.CachingStrategy cache) {
        this.name = name;
        this.cache = PreBuiltCacheFactory.getCache(cache);
    }

    protected PreConfiguredAnalysisComponent(String name, PreBuiltCacheFactory.PreBuiltCache<T> cache) {
        this.name = name;
        this.cache = cache;
    }

    @Override
    public T get(IndexSettings indexSettings, Environment environment, String name, Settings settings) throws IOException {
        Version versionCreated = Version.indexCreated(settings);
        synchronized (this) {
            T factory = cache.get(versionCreated);
            if (factory == null) {
                factory = create(versionCreated);
                cache.put(versionCreated, factory);
            }
            return factory;
        }
    }

    /**
     * The name of the analysis component in the API.
     */
    public String getName() {
        return name;
    }

    protected abstract T create(Version version);
}
