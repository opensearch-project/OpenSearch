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
import org.opensearch.common.settings.Settings;
import org.opensearch.index.AbstractIndexComponent;
import org.opensearch.index.IndexSettings;

public abstract class AbstractIndexAnalyzerProvider<T extends Analyzer> extends AbstractIndexComponent implements AnalyzerProvider<T> {

    private final String name;

    /**
     * Constructs a new analyzer component, with the index name and its settings and the analyzer name.
     *
     * @param indexSettings the settings and the name of the index
     * @param name          The analyzer name
     */
    public AbstractIndexAnalyzerProvider(IndexSettings indexSettings, String name, Settings settings) {
        super(indexSettings);
        this.name = name;
        Analysis.parseAndDeprecateAnalysisVersion(name, settings);
    }

    /**
     * Returns the injected name of the analyzer.
     */
    @Override
    public final String name() {
        return this.name;
    }

    @Override
    public final AnalyzerScope scope() {
        return AnalyzerScope.INDEX;
    }
}
