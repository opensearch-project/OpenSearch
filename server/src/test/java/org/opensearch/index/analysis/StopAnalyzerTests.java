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
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTokenStreamTestCase;

import static org.opensearch.test.OpenSearchTestCase.createTestAnalysis;

public class StopAnalyzerTests extends OpenSearchTokenStreamTestCase {
    public void testDefaultsCompoundAnalysis() throws Exception {
        String json = "/org/opensearch/index/analysis/stop.json";
        Settings settings = Settings.builder()
            .loadFromStream(json, getClass().getResourceAsStream(json), false)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        IndexAnalyzers indexAnalyzers = createTestAnalysis(idxSettings, settings).indexAnalyzers;
        NamedAnalyzer analyzer1 = indexAnalyzers.get("analyzer1");

        assertTokenStreamContents(analyzer1.tokenStream("test", "to be or not to be"), new String[0]);

        NamedAnalyzer analyzer2 = indexAnalyzers.get("analyzer2");

        assertTokenStreamContents(analyzer2.tokenStream("test", "to be or not to be"), new String[0]);
    }
}
