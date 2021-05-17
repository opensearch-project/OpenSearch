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

package org.opensearch.analysis.common;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.FingerprintFilter;
import org.opensearch.LegacyESVersion;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AbstractTokenFilterFactory;
import org.opensearch.index.analysis.TokenFilterFactory;

import static org.opensearch.analysis.common.FingerprintAnalyzerProvider.DEFAULT_MAX_OUTPUT_SIZE;
import static org.opensearch.analysis.common.FingerprintAnalyzerProvider.MAX_OUTPUT_SIZE;

public class FingerprintTokenFilterFactory extends AbstractTokenFilterFactory {

    private static final DeprecationLogger DEPRECATION_LOGGER =  DeprecationLogger.getLogger(FingerprintTokenFilterFactory.class);

    private final char separator;
    private final int maxOutputSize;

    FingerprintTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
        this.separator = FingerprintAnalyzerProvider.parseSeparator(settings);
        this.maxOutputSize = settings.getAsInt(MAX_OUTPUT_SIZE.getPreferredName(), DEFAULT_MAX_OUTPUT_SIZE);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        TokenStream result = tokenStream;
        result = new FingerprintFilter(result, maxOutputSize, separator);
        return result;
    }

    @Override
    public TokenFilterFactory getSynonymFilter() {
        if (indexSettings.getIndexVersionCreated().onOrAfter(LegacyESVersion.V_7_0_0)) {
            throw new IllegalArgumentException("Token filter [" + name() + "] cannot be used to parse synonyms");
        }
        else {
            DEPRECATION_LOGGER.deprecate("synonym_tokenfilters", "Token filter [" + name()
                + "] will not be usable to parse synonyms after v7.0");
            return this;
        }
    }

}
