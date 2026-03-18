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
import org.opensearch.index.IndexSettings;
import org.opensearch.indices.analysis.HunspellService;

import java.util.Locale;
import java.util.regex.Pattern;

/**
 * The token filter factory for the hunspell analyzer
 *
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
 * // Package-based (loads from config/analyzers/pkg-1234/hunspell/en_US/)
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

    public HunspellTokenFilterFactory(IndexSettings indexSettings, String name, Settings settings, HunspellService hunspellService) {
        super(indexSettings, name, settings);

        // Get both ref_path and locale parameters
        String refPath = settings.get("ref_path");  // Package ID only (optional)
        String locale = settings.get("locale", settings.get("language", settings.get("lang", null)));

        if (refPath != null) {
            // Package-based loading: ref_path (package ID) + locale (required)
            if (locale == null) {
                throw new IllegalArgumentException("When using ref_path, the 'locale' parameter is required for hunspell token filter");
            }

            // Validate ref_path and locale are safe package/locale identifiers
            validatePackageIdentifier(refPath, "ref_path");
            validatePackageIdentifier(locale, "locale");

            // Load from package directory: config/analyzers/{ref_path}/hunspell/{locale}/
            dictionary = hunspellService.getDictionaryFromPackage(refPath, locale);
        } else if (locale != null) {
            // Traditional locale-based loading (backward compatible)
            // Loads from config/hunspell/{locale}/
            // Validate locale to prevent path traversal and cache key ambiguity
            validatePackageIdentifier(locale, "locale");
            dictionary = hunspellService.getDictionary(locale);
        } else {
            throw new IllegalArgumentException(
                "The 'locale' parameter is required for hunspell token filter. Set it to the hunspell dictionary locale (e.g., 'en_US')."
            );
        }

        dedup = settings.getAsBoolean("dedup", true);
        longestOnly = settings.getAsBoolean("longest_only", false);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new HunspellStemFilter(tokenStream, dictionary, dedup, longestOnly);
    }

    public boolean dedup() {
        return dedup;
    }

    public boolean longestOnly() {
        return longestOnly;
    }

    /**
     * Allowlist pattern for safe package identifiers and locales.
     * Permits only alphanumeric characters, hyphens, and underscores.
     * Examples: "pkg-1234", "en_US", "my-package-v2", "en_US_custom"
     */
    private static final Pattern SAFE_IDENTIFIER_PATTERN = Pattern.compile("^[a-zA-Z0-9][a-zA-Z0-9_-]*$|^[a-zA-Z0-9]$");

    /**
     * Validates that a package identifier or locale contains only safe characters.
     * Uses an allowlist approach: only alphanumeric characters, hyphens, and underscores are permitted.
     * This prevents path traversal, cache key injection, and other security issues.
     *
     * @param value The value to validate (package ID or locale)
     * @param paramName The parameter name for error messages
     * @throws IllegalArgumentException if validation fails
     */
    static void validatePackageIdentifier(String value, String paramName) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Invalid %s: value cannot be null or empty.", paramName));
        }

        if (!SAFE_IDENTIFIER_PATTERN.matcher(value).matches()) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Invalid %s: [%s]. Only alphanumeric characters, hyphens, and underscores are allowed.",
                    paramName,
                    value
                )
            );
        }

    }

}
