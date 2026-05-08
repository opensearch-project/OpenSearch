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
 *   <li>A ref_path (ref_path, e.g., "analyzers/my-dict") combined with locale for directory-based dictionaries</li>
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
 * // Directory-based (loads from config/analyzers/my-dict/hunspell/en_US/)
 * {
 *   "type": "hunspell",
 *   "ref_path": "analyzers/my-dict",
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
        String refPath = settings.get("ref_path");
        String locale = settings.get("locale", settings.get("language", settings.get("lang", null)));

        if (refPath != null) {
            // Directory-based loading: ref_path + locale (required)
            if (locale == null) {
                throw new IllegalArgumentException("When using ref_path, the 'locale' parameter is required for hunspell token filter");
            }

            // Validate ref_path and locale
            validateRefPath(refPath);
            validateLocale(locale);

            // Load from directory: config/{ref_path}/hunspell/{locale}/
            dictionary = hunspellService.getDictionaryFromRefPath(refPath, locale);
        } else if (locale != null) {
            // Traditional locale-based loading (backward compatible)
            // Loads from config/hunspell/{locale}/
            // Validate locale to prevent path traversal and cache key ambiguity
            validateLocale(locale);
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
     * Allowlist pattern for a ref_path.
     * Permits alphanumeric characters, hyphens, underscores, and forward slashes as path separators.
     * A ref_path is a relative directory path under config/, e.g. "analyzers/my-dict".
     */
    private static final Pattern SAFE_REF_PATH_PATTERN = Pattern.compile("^[a-zA-Z0-9][a-zA-Z0-9_/-]*[a-zA-Z0-9]$|^[a-zA-Z0-9]$");

    /**
     * Allowlist pattern for a locale.
     * Permits alphanumeric characters, hyphens, and underscores.
     * Disallows forward slashes and dots — a locale is a single directory-name segment, e.g. "en_US" or "en_US_custom".
     */
    private static final Pattern SAFE_LOCALE_PATTERN = Pattern.compile("^[a-zA-Z0-9][a-zA-Z0-9_-]*[a-zA-Z0-9]$|^[a-zA-Z0-9]$");

    /**
     * Validates a ref_path value. Allows "/" as a path separator so that callers can pass nested
     * directory paths (e.g. "analyzers/my-dict"). Uses an allowlist to prevent path traversal,
     * cache key injection, and other security issues.
     *
     * @param value the ref_path to validate
     * @throws IllegalArgumentException if validation fails
     */
    static void validateRefPath(String value) {
        validateAgainstPattern(
            value,
            "ref_path",
            SAFE_REF_PATH_PATTERN,
            "Only alphanumeric characters, hyphens, underscores, and forward slashes are allowed."
        );
    }

    /**
     * Validates a locale value. Does not allow "/" — a locale must be a single directory-name segment
     * (e.g. "en_US"). Uses an allowlist to prevent path traversal, cache key injection, and other
     * security issues.
     *
     * @param value the locale to validate
     * @throws IllegalArgumentException if validation fails
     */
    static void validateLocale(String value) {
        validateAgainstPattern(value, "locale", SAFE_LOCALE_PATTERN, "Only alphanumeric characters, hyphens, and underscores are allowed.");
    }

    private static void validateAgainstPattern(String value, String paramName, Pattern pattern, String allowedDesc) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Invalid %s: value cannot be null or empty.", paramName));
        }

        if (!pattern.matcher(value).matches()) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Invalid %s: [%s]. %s", paramName, value, allowedDesc));
        }

        // Additional check: reject ".." sequences even within otherwise valid characters (e.g., "foo..bar")
        if (value.contains("..")) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Invalid %s: [%s]. Consecutive dots ('..') are not allowed.", paramName, value)
            );
        }
    }

}
