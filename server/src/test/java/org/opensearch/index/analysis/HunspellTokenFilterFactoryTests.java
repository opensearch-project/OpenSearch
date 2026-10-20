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

import org.apache.lucene.analysis.hunspell.Dictionary;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.indices.analysis.HunspellService;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

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
     * Test that ref_path with locale loads dictionary from the ref_path directory.
     * Expected: config/{ref_path}/hunspell/{locale}/
     */
    public void testRefPathWithLocaleLoadsDictionaryFromDirectory() throws IOException {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put("index.analysis.filter.my_hunspell.type", "hunspell")
            .put("index.analysis.filter.my_hunspell.ref_path", "analyzers/test-dict")
            .put("index.analysis.filter.my_hunspell.locale", "en_US")
            .build();

        TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, getDataPath("/indices/analyze/conf_dir"));
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_hunspell");
        assertThat(tokenFilter, instanceOf(HunspellTokenFilterFactory.class));
        HunspellTokenFilterFactory hunspellTokenFilter = (HunspellTokenFilterFactory) tokenFilter;
        assertThat(hunspellTokenFilter.dedup(), is(true));
    }

    /**
     * Test that ref_path without locale throws IllegalArgumentException.
     * The locale is required when using ref_path.
     */
    public void testRefPathWithoutLocaleThrowsException() throws IOException {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put("index.analysis.filter.my_hunspell.type", "hunspell")
            .put("index.analysis.filter.my_hunspell.ref_path", "analyzers/test-dict")
            // locale intentionally omitted
            .build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> AnalysisTestsHelper.createTestAnalysisFromSettings(settings, getDataPath("/indices/analyze/conf_dir"))
        );
        assertThat(e.getMessage(), containsString("locale"));
        assertThat(e.getMessage(), containsString("required"));
    }

    /**
     * Test that non-existent ref_path directory throws exception.
     */
    public void testNonExistentRefPathThrowsException() throws IOException {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put("index.analysis.filter.my_hunspell.type", "hunspell")
            .put("index.analysis.filter.my_hunspell.ref_path", "non-existent-dict")
            .put("index.analysis.filter.my_hunspell.locale", "en_US")
            .build();

        Exception e = expectThrows(
            Exception.class,
            () -> AnalysisTestsHelper.createTestAnalysisFromSettings(settings, getDataPath("/indices/analyze/conf_dir"))
        );
        // The exception message should indicate the ref_path or dictionary was not found
        assertThat(e.getMessage(), containsString("non-existent-dict"));
    }

    /**
     * Test that non-existent locale in ref_path throws exception.
     */
    public void testNonExistentLocaleInRefPathThrowsException() throws IOException {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put("index.analysis.filter.my_hunspell.type", "hunspell")
            .put("index.analysis.filter.my_hunspell.ref_path", "analyzers/test-dict")
            .put("index.analysis.filter.my_hunspell.locale", "fr_FR")  // locale doesn't exist in test-dict
            .build();

        Exception e = expectThrows(
            Exception.class,
            () -> AnalysisTestsHelper.createTestAnalysisFromSettings(settings, getDataPath("/indices/analyze/conf_dir"))
        );
        // The exception message should indicate the locale was not found
        assertThat(e.getMessage(), containsString("fr_FR"));
    }

    /**
     * Test dedup and longestOnly settings work with ref_path.
     */
    public void testRefPathWithDedupAndLongestOnly() throws IOException {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put("index.analysis.filter.my_hunspell.type", "hunspell")
            .put("index.analysis.filter.my_hunspell.ref_path", "analyzers/test-dict")
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
     * Test validateRefPath/validateLocale accepts valid identifiers.
     */
    public void testValidateRefPathAndLocaleAcceptsValid() {
        // These should not throw
        HunspellTokenFilterFactory.validateRefPath("analyzers/my-dict");
        HunspellTokenFilterFactory.validateLocale("en_US");
        HunspellTokenFilterFactory.validateRefPath("my-dict-v2");
        HunspellTokenFilterFactory.validateLocale("en_US_custom");
        HunspellTokenFilterFactory.validateRefPath("a"); // single char
        HunspellTokenFilterFactory.validateRefPath("AB"); // two chars
        HunspellTokenFilterFactory.validateRefPath("dict-v1"); // hyphen in middle
    }

    /**
     * Test validateRefPath/validateLocale rejects null.
     */
    public void testValidateRefPathRejectsNull() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> HunspellTokenFilterFactory.validateRefPath(null));
        assertThat(e.getMessage(), containsString("null or empty"));
    }

    /**
     * Test validateRefPath/validateLocale rejects empty string.
     */
    public void testValidateRefPathRejectsEmpty() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> HunspellTokenFilterFactory.validateRefPath(""));
        assertThat(e.getMessage(), containsString("null or empty"));
    }

    /**
     * Test validateRefPath/validateLocale rejects backslash.
     */
    public void testValidateRefPathRejectsBackslash() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> HunspellTokenFilterFactory.validateRefPath("foo\\bar")
        );
        assertThat(e.getMessage(), containsString("Only alphanumeric"));
    }

    /**
     * Test validateRefPath/validateLocale rejects colon (cache key separator).
     */
    public void testValidateRefPathRejectsColon() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> HunspellTokenFilterFactory.validateRefPath("dict:inject")
        );
        assertThat(e.getMessage(), containsString("Only alphanumeric"));
    }

    /**
     * Test validateRefPath/validateLocale rejects leading dot.
     */
    public void testValidateRefPathRejectsLeadingDot() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> HunspellTokenFilterFactory.validateRefPath(".hidden")
        );
        assertThat(e.getMessage(), containsString("Only alphanumeric"));
    }

    /**
     * Test validateRefPath/validateLocale rejects trailing dot.
     */
    public void testValidateRefPathRejectsTrailingDot() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> HunspellTokenFilterFactory.validateRefPath("dict.")
        );
        assertThat(e.getMessage(), containsString("Only alphanumeric"));
    }

    /**
     * Test validateRefPath/validateLocale rejects double dots (path traversal).
     */
    public void testValidateLocaleRejectsDoubleDots() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> HunspellTokenFilterFactory.validateLocale("foo..bar")
        );
        assertThat(e.getMessage(), containsString("Only alphanumeric characters, hyphens, and underscores are allowed."));
    }

    /**
     * Test validateRefPath/validateLocale rejects ".." (pure path traversal).
     */
    public void testValidateRefPathRejectsPureDotDot() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> HunspellTokenFilterFactory.validateRefPath(".."));
        assertThat(e.getMessage(), containsString("Only alphanumeric"));
    }

    /**
     * Test validateRefPath/validateLocale rejects spaces.
     */
    public void testValidateRefPathRejectsSpaces() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> HunspellTokenFilterFactory.validateRefPath("my dict")
        );
        assertThat(e.getMessage(), containsString("Only alphanumeric"));
    }

    /**
     * Test validateRefPath/validateLocale rejects special characters.
     */
    public void testValidateRefPathRejectsSpecialChars() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> HunspellTokenFilterFactory.validateRefPath("dict@v1")
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
            .put("index.analysis.filter.my_hunspell.ref_path", "analyzers/test-dict")
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

    /**
     * Test that updateable flag sets analysis mode to SEARCH_TIME.
     */
    public void testRefPathWithUpdateableFlag() throws IOException {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put("index.analysis.filter.my_hunspell.type", "hunspell")
            .put("index.analysis.filter.my_hunspell.ref_path", "analyzers/test-dict")
            .put("index.analysis.filter.my_hunspell.locale", "en_US")
            .put("index.analysis.filter.my_hunspell.updateable", true)
            .build();

        TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, getDataPath("/indices/analyze/conf_dir"));
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_hunspell");
        assertThat(tokenFilter, instanceOf(HunspellTokenFilterFactory.class));
        HunspellTokenFilterFactory hunspellTokenFilter = (HunspellTokenFilterFactory) tokenFilter;

        assertThat(hunspellTokenFilter.getAnalysisMode(), is(AnalysisMode.SEARCH_TIME));
    }

    /**
     * Test that without updateable flag, analysis mode is ALL (default).
     */
    public void testRefPathWithoutUpdateableFlagDefaultsToAllMode() throws IOException {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put("index.analysis.filter.my_hunspell.type", "hunspell")
            .put("index.analysis.filter.my_hunspell.ref_path", "analyzers/test-dict")
            .put("index.analysis.filter.my_hunspell.locale", "en_US")
            .build();

        TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, getDataPath("/indices/analyze/conf_dir"));
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_hunspell");
        assertThat(tokenFilter, instanceOf(HunspellTokenFilterFactory.class));
        HunspellTokenFilterFactory hunspellTokenFilter = (HunspellTokenFilterFactory) tokenFilter;

        assertThat(hunspellTokenFilter.getAnalysisMode(), is(AnalysisMode.ALL));
    }

    /**
     * Test that reloadCachedResources() reloads the ref_path dictionary.
     */
    public void testReloadCachedResourcesRefPath() throws Exception {
        Path tempDir = createTempDir();
        Path dictDir = tempDir.resolve("config").resolve("analyzers/test-dict").resolve("hunspell").resolve("en_US");
        Files.createDirectories(dictDir);
        Files.write(dictDir.resolve("en_US.aff"), Collections.singletonList("SET UTF-8"));
        Files.write(dictDir.resolve("en_US.dic"), java.util.Arrays.asList("1", "test"));

        Settings nodeSettings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), tempDir).build();
        Environment env = new Environment(nodeSettings, tempDir.resolve("config"));
        HunspellService hunspellService = new HunspellService(nodeSettings, env, Collections.emptyMap());

        IndexSettings idx = IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder().put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT).build()
        );
        Settings filterSettings = Settings.builder().put("ref_path", "analyzers/test-dict").put("locale", "en_US").build();

        HunspellTokenFilterFactory factory = new HunspellTokenFilterFactory(idx, "my_hunspell", filterSettings, hunspellService);

        Dictionary dict1 = hunspellService.getDictionaryFromRefPath("analyzers/test-dict", "en_US");
        assertNotNull(dict1);

        factory.reloadCachedResources();
        Dictionary dict2 = hunspellService.getDictionaryFromRefPath("analyzers/test-dict", "en_US");
        assertNotNull(dict2);
        assertNotSame("Expected fresh Dictionary instance after reloadCachedResources()", dict1, dict2);
    }

    /**
     * Test that reloadCachedResources() reloads the traditional locale dictionary.
     */
    public void testReloadCachedResourcesTraditionalLocale() throws Exception {
        Path tempDir = createTempDir();
        Path dir = tempDir.resolve("config").resolve("hunspell").resolve("en_US");
        Files.createDirectories(dir);
        Files.write(dir.resolve("en_US.aff"), Collections.singletonList("SET UTF-8"));
        Files.write(dir.resolve("en_US.dic"), java.util.Arrays.asList("1", "test"));

        Settings nodeSettings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), tempDir).build();
        Environment env = new Environment(nodeSettings, tempDir.resolve("config"));
        HunspellService hunspellService = new HunspellService(nodeSettings, env, Collections.emptyMap());

        IndexSettings idx = IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder().put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT).build()
        );
        Settings filterSettings = Settings.builder().put("locale", "en_US").build();

        HunspellTokenFilterFactory factory = new HunspellTokenFilterFactory(idx, "my_hunspell", filterSettings, hunspellService);

        Dictionary dict1 = hunspellService.getDictionary("en_US");
        assertNotNull(dict1);

        factory.reloadCachedResources();
        Dictionary dict2 = hunspellService.getDictionary("en_US");
        assertNotNull(dict2);
        assertNotSame("Expected fresh Dictionary instance after reloadCachedResources()", dict1, dict2);
    }

    /**
     * Simulates the MapperService.reloadSearchAnalyzers(registry, true) cacheReload walk.
     * Verifies that iterating token filters and calling reloadCachedResources() on HunspellTokenFilterFactory
     * correctly evicts the cached dictionary.
     */
    public void testMapperServiceReloadWalk() throws Exception {
        Path tempDir = createTempDir();
        Path dictDir = tempDir.resolve("config").resolve("analyzers/test-dict").resolve("hunspell").resolve("en_US");
        Files.createDirectories(dictDir);
        Files.write(dictDir.resolve("en_US.aff"), Collections.singletonList("SET UTF-8"));
        Files.write(dictDir.resolve("en_US.dic"), java.util.Arrays.asList("1", "test"));

        Settings nodeSettings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), tempDir).build();
        Environment env = new Environment(nodeSettings, tempDir.resolve("config"));
        HunspellService hunspellService = new HunspellService(nodeSettings, env, Collections.emptyMap());

        IndexSettings idx = IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder().put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT).build()
        );
        Settings filterSettings = Settings.builder()
            .put("ref_path", "analyzers/test-dict")
            .put("locale", "en_US")
            .put("updateable", true)
            .build();

        HunspellTokenFilterFactory hunspellFactory = new HunspellTokenFilterFactory(idx, "my_hunspell", filterSettings, hunspellService);

        // Pre-load into cache
        Dictionary dict1 = hunspellService.getDictionaryFromRefPath("analyzers/test-dict", "en_US");
        assertNotNull(dict1);

        // Reload cached resources
        hunspellFactory.reloadCachedResources();

        // Verify eviction
        Dictionary dict2 = hunspellService.getDictionaryFromRefPath("analyzers/test-dict", "en_US");
        assertNotSame("Dictionary should be fresh after reload walk", dict1, dict2);
    }

}
