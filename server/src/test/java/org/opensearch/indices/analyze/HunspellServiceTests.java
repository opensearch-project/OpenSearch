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

package org.opensearch.indices.analyze;

import org.apache.lucene.analysis.hunspell.Dictionary;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.indices.analysis.HunspellService;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;

import static java.util.Collections.emptyMap;
import static org.opensearch.indices.analysis.HunspellService.HUNSPELL_IGNORE_CASE;
import static org.opensearch.indices.analysis.HunspellService.HUNSPELL_LAZY_LOAD;
import static org.hamcrest.Matchers.notNullValue;

public class HunspellServiceTests extends OpenSearchTestCase {
    public void testLocaleDirectoryWithNodeLevelConfig() throws Exception {
        Settings settings = Settings.builder()
            .put(HUNSPELL_LAZY_LOAD.getKey(), randomBoolean())
            .put(HUNSPELL_IGNORE_CASE.getKey(), true)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .build();

        final Environment environment = new Environment(settings, getDataPath("/indices/analyze/conf_dir"));
        Dictionary dictionary = new HunspellService(settings, environment, emptyMap()).getDictionary("en_US");
        assertThat(dictionary, notNullValue());
        assertTrue(dictionary.getIgnoreCase());
    }

    public void testLocaleDirectoryWithLocaleSpecificConfig() throws Exception {
        Settings settings = Settings.builder()
            .put(HUNSPELL_LAZY_LOAD.getKey(), randomBoolean())
            .put(HUNSPELL_IGNORE_CASE.getKey(), true)
            .put("indices.analysis.hunspell.dictionary.en_US.strict_affix_parsing", false)
            .put("indices.analysis.hunspell.dictionary.en_US.ignore_case", false)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .build();

        final Path configPath = getDataPath("/indices/analyze/conf_dir");
        final Environment environment = new Environment(settings, configPath);
        Dictionary dictionary = new HunspellService(settings, environment, emptyMap()).getDictionary("en_US");
        assertThat(dictionary, notNullValue());
        assertFalse(dictionary.getIgnoreCase());

        // testing that dictionary specific settings override node level settings
        dictionary = new HunspellService(settings, new Environment(settings, configPath), emptyMap()).getDictionary("en_US_custom");
        assertThat(dictionary, notNullValue());
        assertTrue(dictionary.getIgnoreCase());
    }

    public void testDicWithNoAff() throws Exception {
        Settings settings = Settings.builder()
            .put(HUNSPELL_LAZY_LOAD.getKey(), randomBoolean())
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .build();

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> {
            final Environment environment = new Environment(settings, getDataPath("/indices/analyze/no_aff_conf_dir"));
            new HunspellService(settings, environment, emptyMap()).getDictionary("en_US");
        });
        assertEquals("Failed to load hunspell dictionary for locale: en_US", e.getMessage());
        assertNull(e.getCause());
    }

    public void testDicWithTwoAffs() {
        Settings settings = Settings.builder()
            .put(HUNSPELL_LAZY_LOAD.getKey(), randomBoolean())
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .build();

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> {
            final Environment environment = new Environment(settings, getDataPath("/indices/analyze/two_aff_conf_dir"));
            new HunspellService(settings, environment, emptyMap()).getDictionary("en_US");
        });
        assertEquals("Failed to load hunspell dictionary for locale: en_US", e.getMessage());
        assertNull(e.getCause());
    }

    // ========== REF_PATH (Directory-based Dictionary) TESTS ==========

    public void testGetDictionaryFromRefPath() throws Exception {
        Path tempDir = createTempDir();
        // Create ref_path directory structure: config/analyzers/my-dict/hunspell/en_US/
        Path refPathDir = tempDir.resolve("config").resolve("analyzers/my-dict").resolve("hunspell").resolve("en_US");
        java.nio.file.Files.createDirectories(refPathDir);

        // Create minimal hunspell files
        createHunspellFiles(refPathDir, "en_US");

        Settings settings = Settings.builder()
            .put(HUNSPELL_LAZY_LOAD.getKey(), randomBoolean())
            .put(Environment.PATH_HOME_SETTING.getKey(), tempDir)
            .build();

        Environment environment = new Environment(settings, tempDir.resolve("config"));
        HunspellService hunspellService = new HunspellService(settings, environment, emptyMap());

        // Test getDictionaryFromRefPath
        Dictionary dictionary = hunspellService.getDictionaryFromRefPath("analyzers/my-dict", "en_US");
        assertThat(dictionary, notNullValue());
    }

    public void testGetDictionaryFromRefPathCaching() throws Exception {
        Path tempDir = createTempDir();
        Path refPathDir = tempDir.resolve("config").resolve("analyzers/my-dict").resolve("hunspell").resolve("en_US");
        java.nio.file.Files.createDirectories(refPathDir);
        createHunspellFiles(refPathDir, "en_US");

        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), tempDir).build();

        Environment environment = new Environment(settings, tempDir.resolve("config"));
        HunspellService hunspellService = new HunspellService(settings, environment, emptyMap());

        // First call - loads from disk
        Dictionary dict1 = hunspellService.getDictionaryFromRefPath("analyzers/my-dict", "en_US");
        assertThat(dict1, notNullValue());

        // Second call - should return cached instance
        Dictionary dict2 = hunspellService.getDictionaryFromRefPath("analyzers/my-dict", "en_US");
        assertSame("Should return same cached instance", dict1, dict2);
    }

    public void testMultipleRefPathsCaching() throws Exception {
        Path tempDir = createTempDir();

        // Create two different ref_path directories
        Path pkg1Dir = tempDir.resolve("config").resolve("analyzers/my-dict").resolve("hunspell").resolve("en_US");
        Path pkg2Dir = tempDir.resolve("config").resolve("custom/other-dict").resolve("hunspell").resolve("en_US");
        java.nio.file.Files.createDirectories(pkg1Dir);
        java.nio.file.Files.createDirectories(pkg2Dir);
        createHunspellFiles(pkg1Dir, "en_US");
        createHunspellFiles(pkg2Dir, "en_US");

        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), tempDir).build();

        Environment environment = new Environment(settings, tempDir.resolve("config"));
        HunspellService hunspellService = new HunspellService(settings, environment, emptyMap());

        // Load both ref_path dictionaries
        Dictionary dict1 = hunspellService.getDictionaryFromRefPath("analyzers/my-dict", "en_US");
        Dictionary dict2 = hunspellService.getDictionaryFromRefPath("custom/other-dict", "en_US");

        assertThat(dict1, notNullValue());
        assertThat(dict2, notNullValue());
        assertNotSame("Different ref_paths should have different Dictionary instances", dict1, dict2);

    }

    public void testBuildRefPathCacheKey() {
        assertEquals("analyzers/my-dict:en_US", HunspellService.buildRefPathCacheKey("analyzers/my-dict", "en_US"));
        assertEquals("my-dict:fr_FR", HunspellService.buildRefPathCacheKey("my-dict", "fr_FR"));
    }

    public void testGetDictionaryFromRefPathNotFound() throws Exception {
        Path tempDir = createTempDir();
        // Don't create the ref_path directory - it doesn't exist

        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), tempDir).build();

        Environment environment = new Environment(settings, tempDir.resolve("config"));
        HunspellService hunspellService = new HunspellService(settings, environment, emptyMap());

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> {
            hunspellService.getDictionaryFromRefPath("nonexistent-pkg", "en_US");
        });
        assertTrue(e.getMessage().contains("Failed to load hunspell dictionary for ref_path"));
    }

    public void testMixedCacheKeysTraditionalAndRefPath() throws Exception {
        Path tempDir = createTempDir();

        // Create traditional hunspell directory
        Path traditionalDir = tempDir.resolve("config").resolve("hunspell").resolve("en_US");
        java.nio.file.Files.createDirectories(traditionalDir);
        createHunspellFiles(traditionalDir, "en_US");

        // Create ref_path directory
        Path refPathDir = tempDir.resolve("config").resolve("analyzers/my-dict").resolve("hunspell").resolve("en_US");
        java.nio.file.Files.createDirectories(refPathDir);
        createHunspellFiles(refPathDir, "en_US");

        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), tempDir).build();

        Environment environment = new Environment(settings, tempDir.resolve("config"));
        HunspellService hunspellService = new HunspellService(settings, environment, emptyMap());

        // Load traditional dictionary
        Dictionary traditionalDict = hunspellService.getDictionary("en_US");
        // Load ref_path-based dictionary
        Dictionary refPathDict = hunspellService.getDictionaryFromRefPath("analyzers/my-dict", "en_US");

        assertThat(traditionalDict, notNullValue());
        assertThat(refPathDict, notNullValue());
        assertNotSame("Traditional and ref_path dictionaries should be different instances", traditionalDict, refPathDict);

        // Both cache keys should exist
    }

    public void testGetDictionaryFromRefPathWithNullRefPath() throws Exception {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put(HUNSPELL_LAZY_LOAD.getKey(), true)
            .build();
        Environment environment = new Environment(settings, getDataPath("/indices/analyze/conf_dir"));
        HunspellService hunspellService = new HunspellService(settings, environment, emptyMap());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> hunspellService.getDictionaryFromRefPath(null, "en_US")
        );
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("refPath"));
    }

    public void testGetDictionaryFromRefPathWithEmptyRefPath() throws Exception {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put(HUNSPELL_LAZY_LOAD.getKey(), true)
            .build();
        Environment environment = new Environment(settings, getDataPath("/indices/analyze/conf_dir"));
        HunspellService hunspellService = new HunspellService(settings, environment, emptyMap());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> hunspellService.getDictionaryFromRefPath("", "en_US")
        );
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("refPath"));
    }

    public void testGetDictionaryFromRefPathWithNullLocale() throws Exception {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put(HUNSPELL_LAZY_LOAD.getKey(), true)
            .build();
        Environment environment = new Environment(settings, getDataPath("/indices/analyze/conf_dir"));
        HunspellService hunspellService = new HunspellService(settings, environment, emptyMap());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> hunspellService.getDictionaryFromRefPath("analyzers/test-pkg", null)
        );
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("locale"));
    }

    public void testGetDictionaryFromRefPathWithEmptyLocale() throws Exception {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put(HUNSPELL_LAZY_LOAD.getKey(), true)
            .build();
        Environment environment = new Environment(settings, getDataPath("/indices/analyze/conf_dir"));
        HunspellService hunspellService = new HunspellService(settings, environment, emptyMap());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> hunspellService.getDictionaryFromRefPath("analyzers/test-pkg", "")
        );
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("locale"));
    }

    public void testRefPathWithMissingHunspellSubdir() throws Exception {
        Path tempDir = createTempDir();
        // Create ref_path dir WITHOUT hunspell subdirectory
        Path refPathDir = tempDir.resolve("config").resolve("bad-dict");
        java.nio.file.Files.createDirectories(refPathDir);

        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), tempDir).build();
        Environment environment = new Environment(settings, tempDir.resolve("config"));
        HunspellService hunspellService = new HunspellService(settings, environment, emptyMap());

        Exception e = expectThrows(Exception.class, () -> hunspellService.getDictionaryFromRefPath("bad-dict", "en_US"));
        assertTrue(e.getMessage().contains("bad-dict"));
    }

    public void testRefPathMissingLocaleDir() throws Exception {
        Path tempDir = createTempDir();
        // Create ref_path + hunspell dir but no locale subdir
        Path hunspellDir = tempDir.resolve("config").resolve("empty-dict").resolve("hunspell");
        java.nio.file.Files.createDirectories(hunspellDir);

        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), tempDir).build();
        Environment environment = new Environment(settings, tempDir.resolve("config"));
        HunspellService hunspellService = new HunspellService(settings, environment, emptyMap());

        Exception e = expectThrows(Exception.class, () -> hunspellService.getDictionaryFromRefPath("empty-dict", "en_US"));
        assertTrue(e.getMessage().contains("en_US") || e.getMessage().contains("empty-dict"));
    }

    public void testRefPathMissingAffFile() throws Exception {
        Path tempDir = createTempDir();
        Path localeDir = tempDir.resolve("config").resolve("noaff-dict").resolve("hunspell").resolve("en_US");
        java.nio.file.Files.createDirectories(localeDir);
        // Only create .dic, no .aff
        java.nio.file.Files.write(localeDir.resolve("en_US.dic"), java.util.Arrays.asList("1", "test"));

        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), tempDir).build();
        Environment environment = new Environment(settings, tempDir.resolve("config"));
        HunspellService hunspellService = new HunspellService(settings, environment, emptyMap());

        Exception e = expectThrows(Exception.class, () -> hunspellService.getDictionaryFromRefPath("noaff-dict", "en_US"));
        assertTrue(e.getMessage().contains("affix") || e.getMessage().contains("noaff-dict"));
    }

    public void testPathTraversalInRefPath() throws Exception {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put(HUNSPELL_LAZY_LOAD.getKey(), true)
            .build();
        Environment environment = new Environment(settings, getDataPath("/indices/analyze/conf_dir"));
        HunspellService hunspellService = new HunspellService(settings, environment, emptyMap());

        Exception e = expectThrows(Exception.class, () -> hunspellService.getDictionaryFromRefPath("..", "en_US"));
        assertNotNull(e);
    }

    public void testPathTraversalInLocale() throws Exception {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put(HUNSPELL_LAZY_LOAD.getKey(), true)
            .build();
        Environment environment = new Environment(settings, getDataPath("/indices/analyze/conf_dir"));
        HunspellService hunspellService = new HunspellService(settings, environment, emptyMap());

        Exception e = expectThrows(Exception.class, () -> hunspellService.getDictionaryFromRefPath("analyzers/test-pkg", "../en_US"));
        assertNotNull(e);
    }

    public void testNonExistentRefPathThrowsException() throws Exception {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put(HUNSPELL_LAZY_LOAD.getKey(), true)
            .build();
        Environment environment = new Environment(settings, getDataPath("/indices/analyze/conf_dir"));
        HunspellService hunspellService = new HunspellService(settings, environment, emptyMap());

        Exception e = expectThrows(Exception.class, () -> hunspellService.getDictionaryFromRefPath("foo/bar", "en_US"));
        assertNotNull(e);
    }

    public void testBackslashInLocale() throws Exception {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put(HUNSPELL_LAZY_LOAD.getKey(), true)
            .build();
        Environment environment = new Environment(settings, getDataPath("/indices/analyze/conf_dir"));
        HunspellService hunspellService = new HunspellService(settings, environment, emptyMap());

        Exception e = expectThrows(Exception.class, () -> hunspellService.getDictionaryFromRefPath("analyzers/test-pkg", "en\\US"));
        assertNotNull(e);
    }

    // Helper method to create minimal hunspell files for testing
    private void createHunspellFiles(Path directory, String locale) throws java.io.IOException {
        // Create .aff file
        Path affFile = directory.resolve(locale + ".aff");
        java.nio.file.Files.write(affFile, java.util.Arrays.asList("SET UTF-8", "SFX S Y 1", "SFX S 0 s ."));

        // Create .dic file
        Path dicFile = directory.resolve(locale + ".dic");
        java.nio.file.Files.write(dicFile, java.util.Arrays.asList("3", "test/S", "word/S", "hello"));
    }
}
