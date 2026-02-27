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
import org.opensearch.core.util.FileSystemUtils;
import org.opensearch.env.Environment;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Serves as a node level registry for hunspell dictionaries. This service supports loading dictionaries from:
 * <ul>
 *   <li>Traditional location: {@code <path.conf>/hunspell/<locale>/} (e.g., config/hunspell/en_US/)</li>
 *   <li>Package-based location: {@code <path.conf>/packages/<package-id>/hunspell/<locale>/} (e.g., config/packages/pkg-1234/hunspell/en_US/)</li>
 * </ul>
 * 
 * <h2>Cache Key Strategy:</h2>
 * <ul>
 *   <li>Traditional dictionaries: Cache key = locale (e.g., "en_US")</li>
 *   <li>Package-based dictionaries: Cache key = "{packageId}:{locale}" (e.g., "pkg-1234:en_US")</li>
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
    
    /** Separator used in cache keys for package-based dictionaries: "{packageId}:{locale}" */
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
    private final Function<String, Dictionary> loadingFunction;

    public HunspellService(final Settings settings, final Environment env, final Map<String, Dictionary> knownDictionaries)
        throws IOException {
        this.knownDictionaries = Collections.unmodifiableMap(knownDictionaries);
        this.hunspellDir = resolveHunspellDirectory(env);
        this.defaultIgnoreCase = HUNSPELL_IGNORE_CASE.get(settings);
        this.loadingFunction = (locale) -> {
            try {
                return loadDictionary(locale, settings, env);
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
     * Returns the hunspell dictionary from a package directory.
     * Loads from package location: config/packages/{packageId}/hunspell/{locale}/
     * 
     * <p>Cache key format: "{packageId}:{locale}" (e.g., "pkg-1234:en_US")
     *
     * @param packageId The package ID (e.g., "pkg-1234")
     * @param locale The locale (e.g., "en_US")
     * @param env The environment
     * @return The loaded Dictionary
     * @throws IllegalArgumentException if packageId or locale is null
     * @throws OpenSearchException if hunspell directory not found or dictionary cannot be loaded
     */
    public Dictionary getDictionaryFromPackage(String packageId, String locale, Environment env) {
        if (packageId == null || packageId.isEmpty()) {
            throw new IllegalArgumentException("packageId cannot be null or empty");
        }
        if (locale == null || locale.isEmpty()) {
            throw new IllegalArgumentException("locale cannot be null or empty");
        }
        
        String cacheKey = buildPackageCacheKey(packageId, locale);
        
        return dictionaries.computeIfAbsent(cacheKey, (key) -> {
            try {
                return loadDictionaryFromPackage(packageId, locale, env);
            } catch (Exception e) {
                logger.error("Failed to load hunspell dictionary for package [{}] locale [{}]: {}", 
                            packageId, locale, e.getMessage());
                throw new IllegalStateException(
                    String.format(Locale.ROOT, 
                        "Failed to load hunspell dictionary for package [%s] locale [%s]", 
                        packageId, locale), e);
            }
        });
    }

    /**
     * Loads a hunspell dictionary from a package directory.
     * Auto-detects the hunspell subdirectory within the package.
     */
    private Dictionary loadDictionaryFromPackage(String packageId, String locale, Environment env) throws Exception {
        // Resolve package directory: config/packages/{packageId}/
        Path packageDir = env.configDir()
            .resolve("packages")
            .resolve(packageId);
        
        // Security check: ensure path is under config directory
        if (!packageDir.normalize().startsWith(env.configDir().toAbsolutePath())) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Package path must be under config directory. Package: [%s]", packageId)
            );
        }
        
        // Check if package directory exists
        if (!Files.isDirectory(packageDir)) {
            throw new OpenSearchException(
                String.format(Locale.ROOT, 
                    "Package directory not found: [%s]. Expected at: %s", 
                    packageId, packageDir)
            );
        }
        
        // Auto-detect hunspell directory within package
        Path hunspellDir = packageDir.resolve("hunspell");
        if (!Files.isDirectory(hunspellDir)) {
            throw new OpenSearchException(
                String.format(Locale.ROOT, 
                    "Hunspell directory not found in package [%s]. " +
                    "Expected 'hunspell' subdirectory at: %s", 
                    packageId, hunspellDir)
            );
        }
        
        // Resolve locale directory within hunspell
        Path dicDir = hunspellDir.resolve(locale);
        
        if (logger.isDebugEnabled()) {
            logger.debug("Loading hunspell dictionary from package [{}] locale [{}] at [{}]...", 
                        packageId, locale, dicDir);
        }
        
        if (!FileSystemUtils.isAccessibleDirectory(dicDir, logger)) {
            throw new OpenSearchException(
                String.format(Locale.ROOT, 
                    "Locale [%s] not found in package [%s]. " +
                    "Expected directory at: %s", 
                    locale, packageId, dicDir)
            );
        }

        // Load settings from the locale directory if present
        Settings dictSettings = loadDictionarySettings(dicDir, Settings.EMPTY);
        boolean ignoreCase = dictSettings.getAsBoolean("ignore_case", defaultIgnoreCase);

        // Find affix and dictionary files
        Path[] affixFiles = FileSystemUtils.files(dicDir, "*.aff");
        if (affixFiles.length == 0) {
            throw new OpenSearchException(
                String.format(Locale.ROOT, 
                    "Missing affix file (.aff) for hunspell dictionary in package [%s] locale [%s]", 
                    packageId, locale)
            );
        }
        if (affixFiles.length != 1) {
            throw new OpenSearchException(
                String.format(Locale.ROOT, 
                    "Too many affix files (.aff) found for hunspell dictionary in package [%s] locale [%s]. Expected 1, found %d", 
                    packageId, locale, affixFiles.length)
            );
        }

        Path[] dicFiles = FileSystemUtils.files(dicDir, "*.dic");
        if (dicFiles.length == 0) {
            throw new OpenSearchException(
                String.format(Locale.ROOT, 
                    "Missing dictionary file (.dic) for hunspell dictionary in package [%s] locale [%s]", 
                    packageId, locale)
            );
        }

        InputStream affixStream = null;
        List<InputStream> dicStreams = new ArrayList<>(dicFiles.length);
        
        try {
            for (Path dicFile : dicFiles) {
                dicStreams.add(Files.newInputStream(dicFile));
            }
            affixStream = Files.newInputStream(affixFiles[0]);

            try (Directory tmp = new NIOFSDirectory(env.tmpDir())) {
                return new Dictionary(tmp, "hunspell", affixStream, dicStreams, ignoreCase);
            }
        } catch (Exception e) {
            logger.error(() -> new ParameterizedMessage(
                "Could not load hunspell dictionary from package [{}] locale [{}]", packageId, locale), e);
            throw e;
        } finally {
            IOUtils.close(affixStream);
            IOUtils.close(dicStreams);
        }
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
     * Loads the hunspell dictionary for the given local.
     *
     * @param locale       The locale of the hunspell dictionary to be loaded.
     * @param nodeSettings The node level settings
     * @param env          The node environment (from which the conf path will be resolved)
     * @return The loaded Hunspell dictionary
     * @throws Exception when loading fails (due to IO errors or malformed dictionary files)
     */
    private Dictionary loadDictionary(String locale, Settings nodeSettings, Environment env) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("Loading hunspell dictionary [{}]...", locale);
        }
        Path dicDir = hunspellDir.resolve(locale);
        if (FileSystemUtils.isAccessibleDirectory(dicDir, logger) == false) {
            throw new OpenSearchException(String.format(Locale.ROOT, "Could not find hunspell dictionary [%s]", locale));
        }

        // merging node settings with hunspell dictionary specific settings
        Settings dictSettings = HUNSPELL_DICTIONARY_OPTIONS.get(nodeSettings);
        nodeSettings = loadDictionarySettings(dicDir, dictSettings.getByPrefix(locale + "."));

        boolean ignoreCase = nodeSettings.getAsBoolean("ignore_case", defaultIgnoreCase);

        Path[] affixFiles = FileSystemUtils.files(dicDir, "*.aff");
        if (affixFiles.length == 0) {
            throw new OpenSearchException(String.format(Locale.ROOT, "Missing affix file for hunspell dictionary [%s]", locale));
        }
        if (affixFiles.length != 1) {
            throw new OpenSearchException(String.format(Locale.ROOT, "Too many affix files exist for hunspell dictionary [%s]", locale));
        }
        InputStream affixStream = null;

        Path[] dicFiles = FileSystemUtils.files(dicDir, "*.dic");
        List<InputStream> dicStreams = new ArrayList<>(dicFiles.length);
        try {

            for (int i = 0; i < dicFiles.length; i++) {
                dicStreams.add(Files.newInputStream(dicFiles[i]));
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
    

    // ==================== CACHE KEY UTILITIES ====================

    /**
     * Builds the cache key for a package-based dictionary.
     * Format: "{packageId}:{locale}" (e.g., "pkg-1234:en_US")
     *
     * @param packageId The package ID
     * @param locale The locale
     * @return The cache key
     */
    public static String buildPackageCacheKey(String packageId, String locale) {
        return packageId + CACHE_KEY_SEPARATOR + locale;
    }

    /**
     * Checks if a cache key is a package-based key (contains separator).
     *
     * @param cacheKey The cache key to check
     * @return true if it's a package-based key, false if it's a traditional locale key
     */
    public static boolean isPackageCacheKey(String cacheKey) {
        return cacheKey != null && cacheKey.contains(CACHE_KEY_SEPARATOR);
    }

    // ==================== CACHE INVALIDATION METHODS (Hot-Reload Support) ====================

    /**
     * Invalidates a cached dictionary by its key.
     * <ul>
     *   <li>Package-based: key format is "{packageId}:{locale}" (e.g., "pkg-1234:en_US")</li>
     *   <li>Traditional: key is the locale name (e.g., "en_US")</li>
     * </ul>
     *
     * @param cacheKey The cache key to invalidate
     * @return true if dictionary was found and removed, false otherwise
     */
    public boolean invalidateDictionary(String cacheKey) {
        Dictionary removed = dictionaries.remove(cacheKey);
        if (removed != null) {
            logger.info("Invalidated hunspell dictionary cache for key: {}", cacheKey);
            return true;
        }
        logger.debug("No cached dictionary found for key: {}", cacheKey);
        return false;
    }

    /**
     * Invalidates all cached dictionaries matching a package ID.
     * Useful when a package is updated and all its locales need to be refreshed.
     * 
     * <p>Matches cache keys with format "{packageId}:{locale}" (e.g., "pkg-1234:en_US")
     *
     * @param packageId The package ID (e.g., "pkg-1234")
     * @return count of invalidated cache entries
     */
    public int invalidateDictionariesByPackage(String packageId) {
        if (packageId == null || packageId.isEmpty()) {
            logger.warn("Cannot invalidate dictionaries: packageId is null or empty");
            return 0;
        }
        
        int count = 0;
        String prefix = packageId + CACHE_KEY_SEPARATOR;  // Match keys like "pkg-1234:en_US"
        Iterator<String> keys = dictionaries.keySet().iterator();
        while (keys.hasNext()) {
            String key = keys.next();
            if (key.startsWith(prefix)) {
                keys.remove();
                count++;
                logger.debug("Invalidated hunspell dictionary cache entry: {}", key);
            }
        }
        
        if (count > 0) {
            logger.info("Invalidated {} hunspell dictionary cache entries for package: {}", count, packageId);
        } else {
            logger.debug("No cached dictionaries found for package: {}", packageId);
        }
        return count;
    }

    /**
     * Invalidates all cached dictionaries.
     * Next access will reload from disk (lazy reload).
     * 
     * @return count of invalidated cache entries
     */
    public int invalidateAllDictionaries() {
        int count = dictionaries.size();
        dictionaries.clear();
        logger.info("Invalidated all {} cached hunspell dictionaries", count);
        return count;
    }

    /**
     * Force reloads a dictionary from disk by invalidating cache then loading fresh.
     *
     * @param packageId The package ID (e.g., "pkg-1234")
     * @param locale The locale (e.g., "en_US")
     * @param env The environment
     * @return The newly loaded Dictionary
     */
    public Dictionary reloadDictionaryFromPackage(String packageId, String locale, Environment env) {
        String cacheKey = buildPackageCacheKey(packageId, locale);
        invalidateDictionary(cacheKey);
        return getDictionaryFromPackage(packageId, locale, env);
    }

    /**
     * Returns all currently cached dictionary keys for diagnostics/debugging.
     * <ul>
     *   <li>Package-based keys: "{packageId}:{locale}" (e.g., "pkg-1234:en_US")</li>
     *   <li>Traditional keys: "{locale}" (e.g., "en_US")</li>
     * </ul>
     *
     * @return Unmodifiable set of cache keys
     */
    public Set<String> getCachedDictionaryKeys() {
        return Collections.unmodifiableSet(dictionaries.keySet());
    }

    /**
     * Returns the count of currently cached dictionaries.
     *
     * @return count of cached dictionaries
     */
    public int getCachedDictionaryCount() {
        return dictionaries.size();
    }    
}
