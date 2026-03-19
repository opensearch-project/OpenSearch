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

import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class HunspellTokenFilterFactoryTests extends OpenSearchTestCase {

    public void testDedup() throws IOException {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put("index.analysis.filter.en_US.type", "hunspell")
            .put("index.analysis.filter.en_US.locale", "en_US")
            .build();

        TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, getDataPath("/indices/analyze/conf_dir"));
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("en_US");
        assertThat(tokenFilter, instanceOf(HunspellTokenFilterFactory.class));
        HunspellTokenFilterFactory hunspellTokenFilter = (HunspellTokenFilterFactory) tokenFilter;
        assertThat(hunspellTokenFilter.dedup(), is(true));

        settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put("index.analysis.filter.en_US.type", "hunspell")
            .put("index.analysis.filter.en_US.dedup", false)
            .put("index.analysis.filter.en_US.locale", "en_US")
            .build();

        analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, getDataPath("/indices/analyze/conf_dir"));
        tokenFilter = analysis.tokenFilter.get("en_US");
        assertThat(tokenFilter, instanceOf(HunspellTokenFilterFactory.class));
        hunspellTokenFilter = (HunspellTokenFilterFactory) tokenFilter;
        assertThat(hunspellTokenFilter.dedup(), is(false));
    }

    /**
     * Test dedup and longestOnly settings work with ref_path.
     */
    public void testRefPathWithDedupAndLongestOnly() throws IOException {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put("index.analysis.filter.my_hunspell.type", "hunspell")
            .put("index.analysis.filter.my_hunspell.ref_path", "test-pkg")
            .put("index.analysis.filter.my_hunspell.locale", "en_US")
            .put("index.analysis.filter.my_hunspell.dedup", false)
            .put("index.analysis.filter.my_hunspell.longest_only", true)
            .build();

        TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, getDataPath("/indices/analyze/conf_dir"));
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_hunspell");
        assertThat(tokenFilter, instanceOf(HunspellTokenFilterFactory.class));
        HunspellTokenFilterFactory hunspellTokenFilter = (HunspellTokenFilterFactory) tokenFilter;

        assertThat(hunspellTokenFilter.dedup(), is(false));
        assertThat(hunspellTokenFilter.longestOnly(), is(true));
    }

    /**
     * Test traditional locale-only loading still works (backward compatibility).
     */
    public void testTraditionalLocaleOnlyLoadingStillWorks() throws IOException {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put("index.analysis.filter.my_hunspell.type", "hunspell")
            .put("index.analysis.filter.my_hunspell.locale", "en_US")
            // No ref_path - should load from config/hunspell/en_US/
            .build();

        TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, getDataPath("/indices/analyze/conf_dir"));
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_hunspell");
        assertThat(tokenFilter, instanceOf(HunspellTokenFilterFactory.class));
    }

    /**
     * Test that missing both ref_path and locale throws exception.
     */
    public void testMissingBothRefPathAndLocaleThrowsException() throws IOException {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put("index.analysis.filter.my_hunspell.type", "hunspell")
            .build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> AnalysisTestsHelper.createTestAnalysisFromSettings(settings, getDataPath("/indices/analyze/conf_dir"))
        );
        assertThat(e.getMessage(), containsString("locale"));
    }

    /**
     * Test validatePackageIdentifier accepts valid identifiers.
     */
    public void testValidatePackageIdentifierAcceptsValid() {
        // These should not throw
        HunspellTokenFilterFactory.validatePackageIdentifier("pkg-1234", "ref_path");
        HunspellTokenFilterFactory.validatePackageIdentifier("en_US", "locale");
        HunspellTokenFilterFactory.validatePackageIdentifier("my-package-v2", "ref_path");
        HunspellTokenFilterFactory.validatePackageIdentifier("en_US_custom", "locale");
        HunspellTokenFilterFactory.validatePackageIdentifier("a", "ref_path"); // single char
        HunspellTokenFilterFactory.validatePackageIdentifier("AB", "ref_path"); // two chars
    }

    /**
     * Test validatePackageIdentifier rejects null.
     */
    public void testValidatePackageIdentifierRejectsNull() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> HunspellTokenFilterFactory.validatePackageIdentifier(null, "ref_path")
        );
        assertThat(e.getMessage(), containsString("null or empty"));
    }

    /**
     * Test validatePackageIdentifier rejects empty string.
     */
    public void testValidatePackageIdentifierRejectsEmpty() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> HunspellTokenFilterFactory.validatePackageIdentifier("", "ref_path")
        );
        assertThat(e.getMessage(), containsString("null or empty"));
    }

    /**
     * Test validatePackageIdentifier rejects slash.
     */
    public void testValidatePackageIdentifierRejectsSlash() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> HunspellTokenFilterFactory.validatePackageIdentifier("foo/bar", "ref_path")
        );
        assertThat(e.getMessage(), containsString("Only alphanumeric"));
    }

    /**
     * Test validatePackageIdentifier rejects backslash.
     */
    public void testValidatePackageIdentifierRejectsBackslash() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> HunspellTokenFilterFactory.validatePackageIdentifier("foo\\bar", "ref_path")
        );
        assertThat(e.getMessage(), containsString("Only alphanumeric"));
    }

    /**
     * Test validatePackageIdentifier rejects colon (cache key separator).
     */
    public void testValidatePackageIdentifierRejectsColon() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> HunspellTokenFilterFactory.validatePackageIdentifier("pkg:inject", "ref_path")
        );
        assertThat(e.getMessage(), containsString("Only alphanumeric"));
    }

    /**
     * Test validatePackageIdentifier rejects dots.
     */
    public void testValidatePackageIdentifierRejectsDots() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> HunspellTokenFilterFactory.validatePackageIdentifier("pkg.v1", "ref_path")
        );
        assertThat(e.getMessage(), containsString("Only alphanumeric"));
    }

    /**
     * Test validatePackageIdentifier rejects double dots (path traversal).
     */
    public void testValidatePackageIdentifierRejectsDoubleDots() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> HunspellTokenFilterFactory.validatePackageIdentifier("foo..bar", "ref_path")
        );
        assertThat(e.getMessage(), containsString("Only alphanumeric"));
    }

    /**
     * Test validatePackageIdentifier rejects ".." (pure path traversal).
     */
    public void testValidatePackageIdentifierRejectsPureDotDot() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> HunspellTokenFilterFactory.validatePackageIdentifier("..", "ref_path")
        );
        assertThat(e.getMessage(), containsString("Only alphanumeric"));
    }

    /**
     * Test validatePackageIdentifier rejects spaces.
     */
    public void testValidatePackageIdentifierRejectsSpaces() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> HunspellTokenFilterFactory.validatePackageIdentifier("my package", "ref_path")
        );
        assertThat(e.getMessage(), containsString("Only alphanumeric"));
    }

    /**
     * Test validatePackageIdentifier rejects special characters.
     */
    public void testValidatePackageIdentifierRejectsSpecialChars() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> HunspellTokenFilterFactory.validatePackageIdentifier("pkg@v1", "ref_path")
        );
        assertThat(e.getMessage(), containsString("Only alphanumeric"));
    }

    /**
     * Test that create() method produces a valid HunspellStemFilter token stream.
     */
    public void testCreateProducesTokenStream() throws IOException {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put("index.analysis.filter.my_hunspell.type", "hunspell")
            .put("index.analysis.filter.my_hunspell.ref_path", "test-pkg")
            .put("index.analysis.filter.my_hunspell.locale", "en_US")
            .build();

        TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, getDataPath("/indices/analyze/conf_dir"));
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_hunspell");
        assertThat(tokenFilter, instanceOf(HunspellTokenFilterFactory.class));

        // Call create() to cover the HunspellStemFilter creation line
        org.apache.lucene.analysis.TokenStream ts = tokenFilter.create(new org.apache.lucene.tests.analysis.CannedTokenStream());
        assertNotNull(ts);
    }

    /**
     * Test that traditional locale create() method also works.
     */
    public void testCreateWithTraditionalLocale() throws IOException {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put("index.analysis.filter.my_hunspell.type", "hunspell")
            .put("index.analysis.filter.my_hunspell.locale", "en_US")
            .build();

        TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, getDataPath("/indices/analyze/conf_dir"));
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_hunspell");

        org.apache.lucene.analysis.TokenStream ts = tokenFilter.create(new org.apache.lucene.tests.analysis.CannedTokenStream());
        assertNotNull(ts);
    }

    /**
     * Test that 'language' alias works for locale parameter (backward compatibility).
     */
    public void testLanguageAliasForLocale() throws IOException {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put("index.analysis.filter.my_hunspell.type", "hunspell")
            .put("index.analysis.filter.my_hunspell.language", "en_US")
            .build();

        TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, getDataPath("/indices/analyze/conf_dir"));
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_hunspell");
        assertThat(tokenFilter, instanceOf(HunspellTokenFilterFactory.class));
    }
}
