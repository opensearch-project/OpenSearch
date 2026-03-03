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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.hunspell.Dictionary;
import org.apache.lucene.analysis.hunspell.HunspellStemFilter;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.indices.analysis.HunspellService;

import java.util.Locale;

/**
 * The token filter factory for the hunspell analyzer
 * 
 * Supports hot-reload when used with {@code updateable: true} setting.
 * The dictionary is loaded from either:
 * <ul>
 *   <li>A ref_path (package ID, e.g., "pkg-1234") combined with locale for package-based dictionaries</li>
 *   <li>A locale (e.g., "en_US") for traditional hunspell dictionaries from config/hunspell/</li>
 * </ul>
 * 
 * <h2>Usage Examples:</h2>
 * <pre>
 * // Traditional locale-based (loads from config/hunspell/en_US/)
 * {
 *   "type": "hunspell",
 *   "locale": "en_US"
 * }
 * 
 * // Package-based (loads from config/packages/pkg-1234/hunspell/en_US/)
 * {
 *   "type": "hunspell",
 *   "ref_path": "pkg-1234",
 *   "locale": "en_US"
 * }
 * </pre>
 *
 *
 * @opensearch.internal
 */
public class HunspellTokenFilterFactory extends AbstractTokenFilterFactory {

    private final Dictionary dictionary;
    private final boolean dedup;
    private final boolean longestOnly;
    private final AnalysisMode analysisMode;

    public HunspellTokenFilterFactory(IndexSettings indexSettings, String name, Settings settings, HunspellService hunspellService) {
        super(indexSettings, name, settings);
        // Check for updateable flag - enables hot-reload support (same pattern as SynonymTokenFilterFactory)
        boolean updateable = settings.getAsBoolean("updateable", false);
        this.analysisMode = updateable ? AnalysisMode.SEARCH_TIME : AnalysisMode.ALL;

        // Get both ref_path and locale parameters
        String refPath = settings.get("ref_path");  // Package ID only (optional)
        String locale = settings.get("locale", settings.get("language", settings.get("lang", null)));
        
        if (refPath != null) {
            // Package-based loading: ref_path (package ID) + locale (required)
            if (locale == null) {
                throw new IllegalArgumentException(
                    "When using ref_path, the 'locale' parameter is required for hunspell token filter"
                );
            }
            
            // Validate ref_path is just package ID (no slashes allowed)
            if (refPath.contains("/")) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, 
                        "ref_path should contain only the package ID, not a full path. Got: [%s]. " +
                        "Use ref_path for package ID and locale for the dictionary locale.", 
                        refPath)
                );
            }
            
            // Load from package directory: config/packages/{ref_path}/hunspell/{locale}/
            dictionary = hunspellService.getDictionaryFromPackage(refPath, locale);
            if (dictionary == null) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, 
                        "Could not find hunspell dictionary for locale [%s] in package [%s]", 
                        locale, refPath)
                );
            }
        } else if (locale != null) {
            // Traditional locale-based loading (backward compatible)
            // Loads from config/hunspell/{locale}/
            dictionary = hunspellService.getDictionary(locale);
            if (dictionary == null) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Unknown hunspell dictionary for locale [%s]", locale)
                );
            }
        } else {
            throw new IllegalArgumentException(
                "missing [locale | language | lang] configuration for hunspell token filter"
            );
        }

        dedup = settings.getAsBoolean("dedup", true);
        longestOnly = settings.getAsBoolean("longest_only", false);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new HunspellStemFilter(tokenStream, dictionary, dedup, longestOnly);
    }

    /**
     * Returns the analysis mode for this filter.
     * When {@code updateable: true} is set, returns {@code SEARCH_TIME} which enables hot-reload
     * via the _reload_search_analyzers API.
     */
    @Override
    public AnalysisMode getAnalysisMode() {
        return this.analysisMode;
    }
    
    public boolean dedup() {
        return dedup;
    }

    public boolean longestOnly() {
        return longestOnly;
    }

}
