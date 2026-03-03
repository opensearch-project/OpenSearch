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
     * Test that ref_path with locale loads dictionary from package directory.
     * Expected: config/packages/{ref_path}/hunspell/{locale}/
     */
    public void testRefPathWithLocaleLoadsDictionaryFromPackage() throws IOException {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put("index.analysis.filter.my_hunspell.type", "hunspell")
            .put("index.analysis.filter.my_hunspell.ref_path", "test-pkg")
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
            .put("index.analysis.filter.my_hunspell.ref_path", "test-pkg")
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
     * Test that ref_path containing "/" throws IllegalArgumentException.
     * The ref_path should be just the package ID, not a full path.
     */
    public void testRefPathContainingSlashThrowsException() throws IOException {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put("index.analysis.filter.my_hunspell.type", "hunspell")
            .put("index.analysis.filter.my_hunspell.ref_path", "packages/test-pkg")
            .put("index.analysis.filter.my_hunspell.locale", "en_US")
            .build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> AnalysisTestsHelper.createTestAnalysisFromSettings(settings, getDataPath("/indices/analyze/conf_dir"))
        );
        assertThat(e.getMessage(), containsString("ref_path should contain only the package ID"));
        assertThat(e.getMessage(), containsString("not a full path"));
    }

    /**
     * Test that non-existent package directory throws exception.
     */
    public void testNonExistentPackageThrowsException() throws IOException {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put("index.analysis.filter.my_hunspell.type", "hunspell")
            .put("index.analysis.filter.my_hunspell.ref_path", "non-existent-pkg")
            .put("index.analysis.filter.my_hunspell.locale", "en_US")
            .build();

        Exception e = expectThrows(
            Exception.class,
            () -> AnalysisTestsHelper.createTestAnalysisFromSettings(settings, getDataPath("/indices/analyze/conf_dir"))
        );
        // The exception message should indicate the package or dictionary was not found
        assertThat(e.getMessage(), containsString("non-existent-pkg"));
    }

    /**
     * Test that non-existent locale in package throws exception.
     */
    public void testNonExistentLocaleInPackageThrowsException() throws IOException {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put("index.analysis.filter.my_hunspell.type", "hunspell")
            .put("index.analysis.filter.my_hunspell.ref_path", "test-pkg")
            .put("index.analysis.filter.my_hunspell.locale", "fr_FR")  // locale doesn't exist in test-pkg
            .build();

        Exception e = expectThrows(
            Exception.class,
            () -> AnalysisTestsHelper.createTestAnalysisFromSettings(settings, getDataPath("/indices/analyze/conf_dir"))
        );
        // The exception message should indicate the locale was not found
        assertThat(e.getMessage(), containsString("fr_FR"));
    }

    /**
     * Test that updateable flag works with ref_path for hot-reload support.
     * When updateable=true, analysisMode should be SEARCH_TIME.
     */
    public void testRefPathWithUpdateableFlag() throws IOException {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put("index.analysis.filter.my_hunspell.type", "hunspell")
            .put("index.analysis.filter.my_hunspell.ref_path", "test-pkg")
            .put("index.analysis.filter.my_hunspell.locale", "en_US")
            .put("index.analysis.filter.my_hunspell.updateable", true)
            .build();

        TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, getDataPath("/indices/analyze/conf_dir"));
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_hunspell");
        assertThat(tokenFilter, instanceOf(HunspellTokenFilterFactory.class));
        HunspellTokenFilterFactory hunspellTokenFilter = (HunspellTokenFilterFactory) tokenFilter;
        
        // When updateable=true, analysis mode should be SEARCH_TIME to enable hot-reload
        assertThat(hunspellTokenFilter.getAnalysisMode(), is(AnalysisMode.SEARCH_TIME));
    }

    /**
     * Test that without updateable flag, analysis mode is ALL (default).
     */
    public void testRefPathWithoutUpdateableFlagDefaultsToAllMode() throws IOException {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put("index.analysis.filter.my_hunspell.type", "hunspell")
            .put("index.analysis.filter.my_hunspell.ref_path", "test-pkg")
            .put("index.analysis.filter.my_hunspell.locale", "en_US")
            // updateable not set, defaults to false
            .build();

        TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, getDataPath("/indices/analyze/conf_dir"));
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_hunspell");
        assertThat(tokenFilter, instanceOf(HunspellTokenFilterFactory.class));
        HunspellTokenFilterFactory hunspellTokenFilter = (HunspellTokenFilterFactory) tokenFilter;
        
        // Without updateable, analysis mode should be ALL
        assertThat(hunspellTokenFilter.getAnalysisMode(), is(AnalysisMode.ALL));
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
}