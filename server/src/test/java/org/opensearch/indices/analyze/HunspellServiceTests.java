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

    // ========== REF_PATH (Package-based Dictionary) TESTS ==========

    public void testGetDictionaryFromPackage() throws Exception {
        Path tempDir = createTempDir();
        // Create package directory structure: config/analyzers/pkg-1234/hunspell/en_US/
        Path packageDir = tempDir.resolve("config").resolve("analyzers").resolve("pkg-1234").resolve("hunspell").resolve("en_US");
        java.nio.file.Files.createDirectories(packageDir);

        // Create minimal hunspell files
        createHunspellFiles(packageDir, "en_US");

        Settings settings = Settings.builder()
            .put(HUNSPELL_LAZY_LOAD.getKey(), randomBoolean())
            .put(Environment.PATH_HOME_SETTING.getKey(), tempDir)
            .build();

        Environment environment = new Environment(settings, tempDir.resolve("config"));
        HunspellService hunspellService = new HunspellService(settings, environment, emptyMap());

        // Test getDictionaryFromPackage
        Dictionary dictionary = hunspellService.getDictionaryFromPackage("pkg-1234", "en_US");
        assertThat(dictionary, notNullValue());
    }

    public void testGetDictionaryFromPackageCaching() throws Exception {
        Path tempDir = createTempDir();
        Path packageDir = tempDir.resolve("config").resolve("analyzers").resolve("pkg-1234").resolve("hunspell").resolve("en_US");
        java.nio.file.Files.createDirectories(packageDir);
        createHunspellFiles(packageDir, "en_US");

        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), tempDir).build();

        Environment environment = new Environment(settings, tempDir.resolve("config"));
        HunspellService hunspellService = new HunspellService(settings, environment, emptyMap());

        // First call - loads from disk
        Dictionary dict1 = hunspellService.getDictionaryFromPackage("pkg-1234", "en_US");
        assertThat(dict1, notNullValue());

        // Second call - should return cached instance
        Dictionary dict2 = hunspellService.getDictionaryFromPackage("pkg-1234", "en_US");
        assertSame("Should return same cached instance", dict1, dict2);
    }

    public void testMultiplePackagesCaching() throws Exception {
        Path tempDir = createTempDir();

        // Create two different package directories
        Path pkg1Dir = tempDir.resolve("config").resolve("analyzers").resolve("pkg-1234").resolve("hunspell").resolve("en_US");
        Path pkg2Dir = tempDir.resolve("config").resolve("analyzers").resolve("pkg-5678").resolve("hunspell").resolve("en_US");
        java.nio.file.Files.createDirectories(pkg1Dir);
        java.nio.file.Files.createDirectories(pkg2Dir);
        createHunspellFiles(pkg1Dir, "en_US");
        createHunspellFiles(pkg2Dir, "en_US");

        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), tempDir).build();

        Environment environment = new Environment(settings, tempDir.resolve("config"));
        HunspellService hunspellService = new HunspellService(settings, environment, emptyMap());

        // Load both package dictionaries
        Dictionary dict1 = hunspellService.getDictionaryFromPackage("pkg-1234", "en_US");
        Dictionary dict2 = hunspellService.getDictionaryFromPackage("pkg-5678", "en_US");

        assertThat(dict1, notNullValue());
        assertThat(dict2, notNullValue());
        assertNotSame("Different package directories should have different Dictionary instances", dict1, dict2);

    }

    public void testBuildPackageCacheKey() {
        assertEquals("pkg-1234:en_US", HunspellService.buildPackageCacheKey("pkg-1234", "en_US"));
        assertEquals("my-package:fr_FR", HunspellService.buildPackageCacheKey("my-package", "fr_FR"));
    }

    public void testGetDictionaryFromPackageNotFound() throws Exception {
        Path tempDir = createTempDir();
        // Don't create the package directory - it doesn't exist

        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), tempDir).build();

        Environment environment = new Environment(settings, tempDir.resolve("config"));
        HunspellService hunspellService = new HunspellService(settings, environment, emptyMap());

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> {
            hunspellService.getDictionaryFromPackage("nonexistent-pkg", "en_US");
        });
        assertTrue(e.getMessage().contains("Failed to load hunspell dictionary for package"));
    }

    public void testMixedCacheKeysTraditionalAndPackage() throws Exception {
        Path tempDir = createTempDir();

        // Create traditional hunspell directory
        Path traditionalDir = tempDir.resolve("config").resolve("hunspell").resolve("en_US");
        java.nio.file.Files.createDirectories(traditionalDir);
        createHunspellFiles(traditionalDir, "en_US");

        // Create package directory
        Path packageDir = tempDir.resolve("config").resolve("analyzers").resolve("pkg-1234").resolve("hunspell").resolve("en_US");
        java.nio.file.Files.createDirectories(packageDir);
        createHunspellFiles(packageDir, "en_US");

        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), tempDir).build();

        Environment environment = new Environment(settings, tempDir.resolve("config"));
        HunspellService hunspellService = new HunspellService(settings, environment, emptyMap());

        // Load traditional dictionary
        Dictionary traditionalDict = hunspellService.getDictionary("en_US");
        // Load package-based dictionary
        Dictionary packageDict = hunspellService.getDictionaryFromPackage("pkg-1234", "en_US");

        assertThat(traditionalDict, notNullValue());
        assertThat(packageDict, notNullValue());
        assertNotSame("Traditional and package dictionaries should be different instances", traditionalDict, packageDict);

    }

    public void testGetDictionaryFromPackageWithNullPackageId() throws Exception {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put(HUNSPELL_LAZY_LOAD.getKey(), true)
            .build();
        Environment environment = new Environment(settings, getDataPath("/indices/analyze/conf_dir"));
        HunspellService hunspellService = new HunspellService(settings, environment, emptyMap());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> hunspellService.getDictionaryFromPackage(null, "en_US")
        );
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("packageId"));
    }

    public void testGetDictionaryFromPackageWithEmptyPackageId() throws Exception {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put(HUNSPELL_LAZY_LOAD.getKey(), true)
            .build();
        Environment environment = new Environment(settings, getDataPath("/indices/analyze/conf_dir"));
        HunspellService hunspellService = new HunspellService(settings, environment, emptyMap());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> hunspellService.getDictionaryFromPackage("", "en_US")
        );
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("packageId"));
    }

    public void testGetDictionaryFromPackageWithNullLocale() throws Exception {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put(HUNSPELL_LAZY_LOAD.getKey(), true)
            .build();
        Environment environment = new Environment(settings, getDataPath("/indices/analyze/conf_dir"));
        HunspellService hunspellService = new HunspellService(settings, environment, emptyMap());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> hunspellService.getDictionaryFromPackage("test-pkg", null)
        );
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("locale"));
    }

    public void testGetDictionaryFromPackageWithEmptyLocale() throws Exception {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put(HUNSPELL_LAZY_LOAD.getKey(), true)
            .build();
        Environment environment = new Environment(settings, getDataPath("/indices/analyze/conf_dir"));
        HunspellService hunspellService = new HunspellService(settings, environment, emptyMap());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> hunspellService.getDictionaryFromPackage("test-pkg", "")
        );
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("locale"));
    }

    public void testPackageWithMissingHunspellSubdir() throws Exception {
        Path tempDir = createTempDir();
        // Create package dir WITHOUT hunspell subdirectory
        Path packageDir = tempDir.resolve("config").resolve("analyzers").resolve("bad-pkg");
        java.nio.file.Files.createDirectories(packageDir);

        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), tempDir).build();
        Environment environment = new Environment(settings, tempDir.resolve("config"));
        HunspellService hunspellService = new HunspellService(settings, environment, emptyMap());

        Exception e = expectThrows(Exception.class, () -> hunspellService.getDictionaryFromPackage("bad-pkg", "en_US"));
        assertTrue(e.getMessage().contains("bad-pkg"));
    }

    public void testPackageMissingLocaleDir() throws Exception {
        Path tempDir = createTempDir();
        // Create package + hunspell dir but no locale subdir
        Path hunspellDir = tempDir.resolve("config").resolve("analyzers").resolve("pkg-empty").resolve("hunspell");
        java.nio.file.Files.createDirectories(hunspellDir);

        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), tempDir).build();
        Environment environment = new Environment(settings, tempDir.resolve("config"));
        HunspellService hunspellService = new HunspellService(settings, environment, emptyMap());

        Exception e = expectThrows(Exception.class, () -> hunspellService.getDictionaryFromPackage("pkg-empty", "en_US"));
        assertTrue(e.getMessage().contains("en_US") || e.getMessage().contains("pkg-empty"));
    }

    public void testPackageMissingAffFile() throws Exception {
        Path tempDir = createTempDir();
        Path localeDir = tempDir.resolve("config").resolve("analyzers").resolve("pkg-noaff").resolve("hunspell").resolve("en_US");
        java.nio.file.Files.createDirectories(localeDir);
        // Only create .dic, no .aff
        java.nio.file.Files.write(localeDir.resolve("en_US.dic"), java.util.Arrays.asList("1", "test"));

        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), tempDir).build();
        Environment environment = new Environment(settings, tempDir.resolve("config"));
        HunspellService hunspellService = new HunspellService(settings, environment, emptyMap());

        Exception e = expectThrows(Exception.class, () -> hunspellService.getDictionaryFromPackage("pkg-noaff", "en_US"));
        assertTrue(e.getMessage().contains("affix") || e.getMessage().contains("pkg-noaff"));
    }

    public void testPathTraversalInPackageId() throws Exception {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put(HUNSPELL_LAZY_LOAD.getKey(), true)
            .build();
        Environment environment = new Environment(settings, getDataPath("/indices/analyze/conf_dir"));
        HunspellService hunspellService = new HunspellService(settings, environment, emptyMap());

        Exception e = expectThrows(Exception.class, () -> hunspellService.getDictionaryFromPackage("..", "en_US"));
        assertNotNull(e);
    }

    public void testPathTraversalInLocale() throws Exception {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put(HUNSPELL_LAZY_LOAD.getKey(), true)
            .build();
        Environment environment = new Environment(settings, getDataPath("/indices/analyze/conf_dir"));
        HunspellService hunspellService = new HunspellService(settings, environment, emptyMap());

        Exception e = expectThrows(Exception.class, () -> hunspellService.getDictionaryFromPackage("test-pkg", "../en_US"));
        assertNotNull(e);
    }

    public void testSlashInPackageId() throws Exception {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put(HUNSPELL_LAZY_LOAD.getKey(), true)
            .build();
        Environment environment = new Environment(settings, getDataPath("/indices/analyze/conf_dir"));
        HunspellService hunspellService = new HunspellService(settings, environment, emptyMap());

        Exception e = expectThrows(Exception.class, () -> hunspellService.getDictionaryFromPackage("foo/bar", "en_US"));
        assertNotNull(e);
    }

    public void testBackslashInLocale() throws Exception {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put(HUNSPELL_LAZY_LOAD.getKey(), true)
            .build();
        Environment environment = new Environment(settings, getDataPath("/indices/analyze/conf_dir"));
        HunspellService hunspellService = new HunspellService(settings, environment, emptyMap());

        Exception e = expectThrows(Exception.class, () -> hunspellService.getDictionaryFromPackage("test-pkg", "en\\US"));
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
