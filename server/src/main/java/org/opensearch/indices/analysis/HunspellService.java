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

package org.opensearch.indices.analysis;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.analysis.hunspell.Dictionary;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.OpenSearchException;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.Strings;
import org.opensearch.core.util.FileSystemUtils;
import org.opensearch.env.Environment;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Serves as a node level registry for hunspell dictionaries. This service supports loading dictionaries from:
 * <ul>
 *   <li>Traditional location: {@code <path.conf>/hunspell/<locale>/} (e.g., config/hunspell/en_US/)</li>
 *   <li>Directory-based location: {@code <path.conf>/<ref_path>/hunspell/<locale>/} (e.g., config/analyzers/my-dict/hunspell/en_US/)</li>
 * </ul>
 *
 * <h2>Cache Key Strategy:</h2>
 * <ul>
 *   <li>Traditional dictionaries: Cache key = locale (e.g., "en_US")</li>
 *   <li>Directory-based dictionaries: Cache key = "{ref_path}:{locale}" (e.g., "analyzers/my-dict:en_US")</li>
 * </ul>
 *
 * <p>The following settings can be set for each dictionary:
 * <ul>
 * <li>{@code ignore_case} - If true, dictionary matching will be case insensitive (defaults to {@code false})</li>
 * <li>{@code strict_affix_parsing} - Determines whether errors while reading a affix rules file will cause exception or simple be ignored
 *      (defaults to {@code true})</li>
 * </ul>
 *
 * <p>These settings can either be configured as node level configuration, such as:
 * <pre><code>
 *     indices.analysis.hunspell.dictionary.en_US.ignore_case: true
 *     indices.analysis.hunspell.dictionary.en_US.strict_affix_parsing: false
 * </code></pre>
 *
 * <p>or, as dedicated configuration per dictionary, placed in a {@code settings.yml} file under the dictionary directory.
 *
 * @see org.opensearch.index.analysis.HunspellTokenFilterFactory
 *
 * @opensearch.internal
 */
public class HunspellService {

    private static final Logger logger = LogManager.getLogger(HunspellService.class);

    /** Separator used in cache keys for directory-based dictionaries: "{refPath}:{locale}" */
    private static final String CACHE_KEY_SEPARATOR = ":";

    public static final Setting<Boolean> HUNSPELL_LAZY_LOAD = Setting.boolSetting(
        "indices.analysis.hunspell.dictionary.lazy",
        Boolean.FALSE,
        Property.NodeScope
    );
    public static final Setting<Boolean> HUNSPELL_IGNORE_CASE = Setting.boolSetting(
        "indices.analysis.hunspell.dictionary.ignore_case",
        Boolean.FALSE,
        Property.NodeScope
    );
    public static final Setting<Settings> HUNSPELL_DICTIONARY_OPTIONS = Setting.groupSetting(
        "indices.analysis.hunspell.dictionary.",
        Property.NodeScope
    );
    private final ConcurrentHashMap<String, Dictionary> dictionaries = new ConcurrentHashMap<>();
    private final Map<String, Dictionary> knownDictionaries;
    private final boolean defaultIgnoreCase;
    private final Path hunspellDir;
    private final Environment env;
    private final Function<String, Dictionary> loadingFunction;

    public HunspellService(final Settings settings, final Environment env, final Map<String, Dictionary> knownDictionaries)
        throws IOException {
        this.knownDictionaries = Collections.unmodifiableMap(knownDictionaries);
        this.env = env;
        this.hunspellDir = resolveHunspellDirectory(env);
        this.defaultIgnoreCase = HUNSPELL_IGNORE_CASE.get(settings);
        this.loadingFunction = (locale) -> {
            try {
                return loadDictionary(locale, settings, env, hunspellDir);
            } catch (Exception e) {
                logger.error("Failed to load hunspell dictionary for locale: " + locale, e);
                throw new IllegalStateException("Failed to load hunspell dictionary for locale: " + locale);
            }
        };
        if (!HUNSPELL_LAZY_LOAD.get(settings)) {
            scanAndLoadDictionaries();
        }

    }

    /**
     * Returns the hunspell dictionary for the given locale.
     * Loads from traditional location: config/hunspell/{locale}/
     *
     * @param locale The name of the locale (e.g., "en_US")
     * @return The loaded Dictionary
     */
    public Dictionary getDictionary(String locale) {
        Dictionary dictionary = knownDictionaries.get(locale);
        if (dictionary == null) {
            dictionary = dictionaries.computeIfAbsent(locale, loadingFunction);
        }
        return dictionary;
    }

    /**
     * Returns the hunspell dictionary from a directory-based ref_path.
     * Loads from: config/{ref_path}/hunspell/{locale}/
     *
     * <p>Cache key format: "{ref_path}:{locale}" (e.g., "analyzers/my-dict:en_US")
     *
     * @param refPath The ref_path (e.g., "analyzers/my-dict")
     * @param locale The locale (e.g., "en_US")
     * @return The loaded Dictionary
     * @throws IllegalArgumentException if refPath or locale is null
     * @throws IllegalStateException if hunspell directory not found or dictionary cannot be loaded
     */
    public Dictionary getDictionaryFromRefPath(String refPath, String locale) {
        if (Strings.isNullOrEmpty(refPath)) {
            throw new IllegalArgumentException("refPath cannot be null or empty");
        }
        if (Strings.isNullOrEmpty(locale)) {
            throw new IllegalArgumentException("locale cannot be null or empty");
        }

        String cacheKey = buildRefPathCacheKey(refPath, locale);

        return dictionaries.computeIfAbsent(cacheKey, (key) -> {
            try {
                return loadDictionaryFromRefPath(refPath, locale);
            } catch (Exception e) {

                throw new IllegalStateException(
                    String.format(Locale.ROOT, "Failed to load hunspell dictionary for ref_path [%s] locale [%s]", refPath, locale),
                    e
                );
            }
        });
    }

    /**
     * Loads a hunspell dictionary from a directory-based ref_path.
     * Expects hunspell files at: config/{ref_path}/hunspell/{locale}/
     *
     * @param refPath The relative directory path (e.g., "analyzers/my-dict")
     * @param locale The locale (e.g., "en_US")
     * @return The loaded Dictionary
     * @throws Exception if loading fails
     */
    private Dictionary loadDictionaryFromRefPath(String refPath, String locale) throws Exception {
        // Resolve the full path: config/{ref_path}/hunspell/{locale}/
        Path refDir = env.configDir().resolve(refPath);

        // Security check: ensure resolved path stays under config directory
        Path configDirAbsolute = env.configDir().toAbsolutePath().normalize();
        Path refDirAbsolute = refDir.toAbsolutePath().normalize();
        if (!refDirAbsolute.startsWith(configDirAbsolute)) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "ref_path must resolve under config directory. ref_path: [%s]", refPath)
            );
        }

        // Check if ref_path directory exists
        if (!Files.isDirectory(refDir)) {
            throw new OpenSearchException(
                String.format(Locale.ROOT, "Directory not found for ref_path: [%s]. Expected at: %s", refPath, refDir)
            );
        }

        // Resolve hunspell directory within ref_path
        Path refHunspellDir = refDir.resolve("hunspell");
        if (!Files.isDirectory(refHunspellDir)) {
            throw new OpenSearchException(
                String.format(
                    Locale.ROOT,
                    "Hunspell directory not found at ref_path [%s]. Expected 'hunspell' subdirectory at: %s",
                    refPath,
                    refHunspellDir
                )
            );
        }

        // Resolve locale directory within hunspell
        Path dicDir = refHunspellDir.resolve(locale);

        // Security check: ensure locale path does not escape hunspell directory
        Path hunspellDirAbsolute = refHunspellDir.toAbsolutePath().normalize();
        Path dicDirAbsolute = dicDir.toAbsolutePath().normalize();
        if (!dicDirAbsolute.startsWith(hunspellDirAbsolute)) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Locale path must be under hunspell directory. Locale: [%s]", locale)
            );
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Loading hunspell dictionary from ref_path [{}] locale [{}] at [{}]...", refPath, locale, dicDirAbsolute);
        }

        if (!FileSystemUtils.isAccessibleDirectory(dicDir, logger)) {
            throw new OpenSearchException(
                String.format(
                    Locale.ROOT,
                    "Locale [%s] not found at ref_path [%s]. Expected directory at: %s",
                    locale,
                    refPath,
                    dicDirAbsolute
                )
            );
        }

        // Delegate to loadDictionary with the ref_path's hunspell directory as base
        return loadDictionary(locale, Settings.EMPTY, env, refHunspellDir);
    }

    private Path resolveHunspellDirectory(Environment env) {
        return env.configDir().resolve("hunspell");
    }

    /**
     * Scans the hunspell directory and loads all found dictionaries
     */
    private void scanAndLoadDictionaries() throws IOException {
        if (Files.isDirectory(hunspellDir)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(hunspellDir)) {
                for (Path file : stream) {
                    if (Files.isDirectory(file)) {
                        try (DirectoryStream<Path> inner = Files.newDirectoryStream(hunspellDir.resolve(file), "*.dic")) {
                            if (inner.iterator().hasNext()) { // just making sure it's indeed a dictionary dir
                                try {
                                    getDictionary(file.getFileName().toString());
                                } catch (Exception e) {
                                    // The cache loader throws unchecked exception (see #loadDictionary()),
                                    // here we simply report the exception and continue loading the dictionaries
                                    logger.error(
                                        () -> new ParameterizedMessage("exception while loading dictionary {}", file.getFileName()),
                                        e
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Loads a hunspell dictionary from a base directory by resolving the locale subdirectory,
     * finding .aff and .dic files, and creating the Dictionary object.
     * Used by both traditional locale-based loading (baseDir=hunspellDir) and
     * directory-based ref_path loading (baseDir=refPath's hunspell dir).
     *
     * @param locale       The locale of the hunspell dictionary to be loaded
     * @param nodeSettings The node level settings (pass Settings.EMPTY for ref_path-based loading)
     * @param env          The node environment
     * @param baseDir      The base directory containing locale subdirectories with .aff/.dic files
     * @return The loaded Hunspell dictionary
     * @throws Exception when loading fails
     */
    private Dictionary loadDictionary(String locale, Settings nodeSettings, Environment env, Path baseDir) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("Loading hunspell dictionary [{}] from [{}]...", locale, baseDir);
        }
        Path dicDir = baseDir.resolve(locale);
        if (FileSystemUtils.isAccessibleDirectory(dicDir, logger) == false) {
            throw new OpenSearchException(String.format(Locale.ROOT, "Could not find hunspell dictionary [%s]", locale));
        }

        // Merge node settings with hunspell dictionary specific settings
        Settings dictSettings = HUNSPELL_DICTIONARY_OPTIONS.get(nodeSettings);
        nodeSettings = loadDictionarySettings(dicDir, dictSettings.getByPrefix(locale + "."));
        boolean ignoreCase = nodeSettings.getAsBoolean("ignore_case", defaultIgnoreCase);

        // Find and validate affix files
        Path[] affixFiles = FileSystemUtils.files(dicDir, "*.aff");
        if (affixFiles.length == 0) {
            throw new OpenSearchException(String.format(Locale.ROOT, "Missing affix file for hunspell dictionary [%s]", locale));
        }
        if (affixFiles.length != 1) {
            throw new OpenSearchException(String.format(Locale.ROOT, "Too many affix files exist for hunspell dictionary [%s]", locale));
        }

        // Load dictionary files and create Dictionary object
        Path[] dicFiles = FileSystemUtils.files(dicDir, "*.dic");
        List<InputStream> dicStreams = new ArrayList<>(dicFiles.length);
        InputStream affixStream = null;
        try {
            for (Path dicFile : dicFiles) {
                dicStreams.add(Files.newInputStream(dicFile));
            }
            affixStream = Files.newInputStream(affixFiles[0]);

            try (Directory tmp = new NIOFSDirectory(env.tmpDir())) {
                return new Dictionary(tmp, "hunspell", affixStream, dicStreams, ignoreCase);
            }
        } catch (Exception e) {
            logger.error(() -> new ParameterizedMessage("Could not load hunspell dictionary [{}]", locale), e);
            throw e;
        } finally {
            IOUtils.close(affixStream);
            IOUtils.close(dicStreams);
        }
    }

    /**
     * Each hunspell dictionary directory may contain a {@code settings.yml} which holds dictionary specific settings. Default
     * values for these settings are defined in the given default settings.
     *
     * @param dir      The directory of the dictionary
     * @param defaults The default settings for this dictionary
     * @return The resolved settings.
     */
    private static Settings loadDictionarySettings(Path dir, Settings defaults) throws IOException {
        Path file = dir.resolve("settings.yml");
        if (Files.exists(file)) {
            return Settings.builder().loadFromPath(file).put(defaults).build();
        }

        file = dir.resolve("settings.json");
        if (Files.exists(file)) {
            return Settings.builder().loadFromPath(file).put(defaults).build();
        }

        return defaults;
    }

    // ==================== CACHE UTILITIES ====================

    /**
     * Builds the cache key for a directory-based dictionary.
     * Format: "{ref_path}:{locale}" (e.g., "analyzers/my-dict:en_US")
     *
     * @param refPath The ref_path
     * @param locale The locale
     * @return The cache key
     */
    public static String buildRefPathCacheKey(String refPath, String locale) {
        return refPath + CACHE_KEY_SEPARATOR + locale;
    }

    /**
     * Reloads a directory-based dictionary from disk and atomically replaces the cached entry.
     * The cache key is never empty — the old entry is overwritten in a single put.
     *
     * @param refPath The ref_path (e.g., "analyzers/my-dict")
     * @param locale The locale (e.g., "en_US")
     * @return The freshly loaded Dictionary
     * @throws IllegalStateException if reloading fails
     */
    public Dictionary reloadDictionaryFromRefPath(String refPath, String locale) {
        if (Strings.isNullOrEmpty(refPath)) {
            throw new IllegalArgumentException("refPath cannot be null or empty");
        }
        if (Strings.isNullOrEmpty(locale)) {
            throw new IllegalArgumentException("locale cannot be null or empty");
        }

        String cacheKey = buildRefPathCacheKey(refPath, locale);

        final Dictionary freshDictionary;
        try {
            freshDictionary = loadDictionaryFromRefPath(refPath, locale);
        } catch (Exception e) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "Failed to reload hunspell dictionary for ref_path [%s] locale [%s]", refPath, locale),
                e
            );
        }

        dictionaries.put(cacheKey, freshDictionary);
        logger.debug("Reloaded hunspell dictionary cache for key [{}]", cacheKey);
        return freshDictionary;
    }

    /**
     * Reloads a traditional locale-based dictionary from disk and atomically replaces the cached entry.
     *
     * @param locale The locale (e.g., "en_US")
     * @return The freshly loaded Dictionary
     * @throws IllegalStateException if reloading fails
     */
    public Dictionary reloadDictionary(String locale) {
        if (Strings.isNullOrEmpty(locale)) {
            throw new IllegalArgumentException("locale cannot be null or empty");
        }

        final Dictionary freshDictionary;
        try {
            freshDictionary = loadDictionary(locale, Settings.EMPTY, env, hunspellDir);
        } catch (Exception e) {
            throw new IllegalStateException(String.format(Locale.ROOT, "Failed to reload hunspell dictionary for locale [%s]", locale), e);
        }

        dictionaries.put(locale, freshDictionary);
        logger.debug("Reloaded hunspell dictionary cache for locale [{}]", locale);
        return freshDictionary;
    }

}
