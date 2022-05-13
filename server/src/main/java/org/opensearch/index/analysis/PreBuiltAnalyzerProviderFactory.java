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

import org.apache.lucene.analysis.Analyzer;
import org.opensearch.Version;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.internal.io.IOUtils;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.indices.analysis.PreBuiltAnalyzers;
import org.opensearch.indices.analysis.PreBuiltCacheFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Factory to create the providers for all prebuilt analyzers
 *
 * @opensearch.internal
 */
public class PreBuiltAnalyzerProviderFactory extends PreConfiguredAnalysisComponent<AnalyzerProvider<?>> implements Closeable {

    private final Function<Version, Analyzer> create;
    private final PreBuiltAnalyzerProvider current;

    /**
     * This constructor only exists to expose analyzers defined in {@link PreBuiltAnalyzers} as {@link PreBuiltAnalyzerProviderFactory}.
     */
    PreBuiltAnalyzerProviderFactory(String name, PreBuiltAnalyzers preBuiltAnalyzer) {
        super(name, new PreBuiltAnalyzersDelegateCache(name, preBuiltAnalyzer));
        this.create = preBuiltAnalyzer::getAnalyzer;
        current = new PreBuiltAnalyzerProvider(name, AnalyzerScope.INDICES, preBuiltAnalyzer.getAnalyzer(Version.CURRENT));
    }

    public PreBuiltAnalyzerProviderFactory(String name, PreBuiltCacheFactory.CachingStrategy cache, Supplier<Analyzer> create) {
        super(name, cache);
        this.create = version -> create.get();
        this.current = new PreBuiltAnalyzerProvider(name, AnalyzerScope.INDICES, create.get());
    }

    @Override
    public AnalyzerProvider<?> get(IndexSettings indexSettings, Environment environment, String name, Settings settings)
        throws IOException {
        Version versionCreated = Version.indexCreated(settings);
        if (Version.CURRENT.equals(versionCreated) == false) {
            return super.get(indexSettings, environment, name, settings);
        } else {
            return current;
        }
    }

    @Override
    protected AnalyzerProvider<?> create(Version version) {
        assert Version.CURRENT.equals(version) == false;
        Analyzer analyzer = create.apply(version);
        return new PreBuiltAnalyzerProvider(getName(), AnalyzerScope.INDICES, analyzer);
    }

    @Override
    public void close() throws IOException {
        List<Closeable> closeables = cache.values().stream().map(AnalyzerProvider::get).collect(Collectors.toList());
        closeables.add(current.get());
        IOUtils.close(closeables);
    }

    /**
     *  A special cache that closes the gap between PreBuiltAnalyzers and PreBuiltAnalyzerProviderFactory.
     *
     *  This can be removed when all analyzers have been moved away from PreBuiltAnalyzers to
     *  PreBuiltAnalyzerProviderFactory either in server or analysis-common.
     *
     * @opensearch.internal
     */
    static class PreBuiltAnalyzersDelegateCache implements PreBuiltCacheFactory.PreBuiltCache<AnalyzerProvider<?>> {

        private final String name;
        private final PreBuiltAnalyzers preBuiltAnalyzer;

        private PreBuiltAnalyzersDelegateCache(String name, PreBuiltAnalyzers preBuiltAnalyzer) {
            this.name = name;
            this.preBuiltAnalyzer = preBuiltAnalyzer;
        }

        @Override
        public AnalyzerProvider<?> get(Version version) {
            return new PreBuiltAnalyzerProvider(name, AnalyzerScope.INDICES, preBuiltAnalyzer.getAnalyzer(version));
        }

        @Override
        public void put(Version version, AnalyzerProvider<?> analyzerProvider) {
            // No need to put, because we delegate in get() directly to PreBuiltAnalyzers which already caches.
        }

        @Override
        public Collection<AnalyzerProvider<?>> values() {
            return preBuiltAnalyzer.getCache()
                .values()
                .stream()
                // Wrap the analyzer instance in a PreBuiltAnalyzerProvider, this is what PreBuiltAnalyzerProviderFactory#close expects
                // (other caches are not directly caching analyzers, but analyzer provider instead)
                .map(analyzer -> new PreBuiltAnalyzerProvider(name, AnalyzerScope.INDICES, analyzer))
                .collect(Collectors.toList());
        }

    }
}
