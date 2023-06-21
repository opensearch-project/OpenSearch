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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.apache.lucene.analysis.bg.BulgarianAnalyzer;
import org.apache.lucene.analysis.bn.BengaliAnalyzer;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.analysis.ca.CatalanAnalyzer;
import org.apache.lucene.analysis.ckb.SoraniAnalyzer;
import org.apache.lucene.analysis.cz.CzechAnalyzer;
import org.apache.lucene.analysis.da.DanishAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.el.GreekAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.et.EstonianAnalyzer;
import org.apache.lucene.analysis.eu.BasqueAnalyzer;
import org.apache.lucene.analysis.fa.PersianAnalyzer;
import org.apache.lucene.analysis.fi.FinnishAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.ga.IrishAnalyzer;
import org.apache.lucene.analysis.gl.GalicianAnalyzer;
import org.apache.lucene.analysis.hi.HindiAnalyzer;
import org.apache.lucene.analysis.hu.HungarianAnalyzer;
import org.apache.lucene.analysis.hy.ArmenianAnalyzer;
import org.apache.lucene.analysis.id.IndonesianAnalyzer;
import org.apache.lucene.analysis.it.ItalianAnalyzer;
import org.apache.lucene.analysis.lt.LithuanianAnalyzer;
import org.apache.lucene.analysis.lv.LatvianAnalyzer;
import org.apache.lucene.analysis.nl.DutchAnalyzer;
import org.apache.lucene.analysis.no.NorwegianAnalyzer;
import org.apache.lucene.analysis.pt.PortugueseAnalyzer;
import org.apache.lucene.analysis.ro.RomanianAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.sv.SwedishAnalyzer;
import org.apache.lucene.analysis.th.ThaiAnalyzer;
import org.apache.lucene.analysis.tr.TurkishAnalyzer;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.env.Environment;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.unmodifiableMap;

/**
 * Core analysis class
 *
 * @opensearch.internal
 */
public class Analysis {
    private static final Logger LOGGER = LogManager.getLogger(Analysis.class);

    public static CharArraySet parseStemExclusion(Settings settings, CharArraySet defaultStemExclusion) {
        String value = settings.get("stem_exclusion");
        if ("_none_".equals(value)) {
            return CharArraySet.EMPTY_SET;
        }
        List<String> stemExclusion = settings.getAsList("stem_exclusion", null);
        if (stemExclusion != null) {
            // LUCENE 4 UPGRADE: Should be settings.getAsBoolean("stem_exclusion_case", false)?
            return new CharArraySet(stemExclusion, false);
        } else {
            return defaultStemExclusion;
        }
    }

    public static final Map<String, Set<?>> NAMED_STOP_WORDS;
    static {
        Map<String, Set<?>> namedStopWords = new HashMap<>();
        namedStopWords.put("_arabic_", ArabicAnalyzer.getDefaultStopSet());
        namedStopWords.put("_armenian_", ArmenianAnalyzer.getDefaultStopSet());
        namedStopWords.put("_basque_", BasqueAnalyzer.getDefaultStopSet());
        namedStopWords.put("_bengali_", BengaliAnalyzer.getDefaultStopSet());
        namedStopWords.put("_brazilian_", BrazilianAnalyzer.getDefaultStopSet());
        namedStopWords.put("_bulgarian_", BulgarianAnalyzer.getDefaultStopSet());
        namedStopWords.put("_catalan_", CatalanAnalyzer.getDefaultStopSet());
        namedStopWords.put("_czech_", CzechAnalyzer.getDefaultStopSet());
        namedStopWords.put("_danish_", DanishAnalyzer.getDefaultStopSet());
        namedStopWords.put("_dutch_", DutchAnalyzer.getDefaultStopSet());
        namedStopWords.put("_english_", EnglishAnalyzer.getDefaultStopSet());
        namedStopWords.put("_estonian_", EstonianAnalyzer.getDefaultStopSet());
        namedStopWords.put("_finnish_", FinnishAnalyzer.getDefaultStopSet());
        namedStopWords.put("_french_", FrenchAnalyzer.getDefaultStopSet());
        namedStopWords.put("_galician_", GalicianAnalyzer.getDefaultStopSet());
        namedStopWords.put("_german_", GermanAnalyzer.getDefaultStopSet());
        namedStopWords.put("_greek_", GreekAnalyzer.getDefaultStopSet());
        namedStopWords.put("_hindi_", HindiAnalyzer.getDefaultStopSet());
        namedStopWords.put("_hungarian_", HungarianAnalyzer.getDefaultStopSet());
        namedStopWords.put("_indonesian_", IndonesianAnalyzer.getDefaultStopSet());
        namedStopWords.put("_irish_", IrishAnalyzer.getDefaultStopSet());
        namedStopWords.put("_italian_", ItalianAnalyzer.getDefaultStopSet());
        namedStopWords.put("_latvian_", LatvianAnalyzer.getDefaultStopSet());
        namedStopWords.put("_lithuanian_", LithuanianAnalyzer.getDefaultStopSet());
        namedStopWords.put("_norwegian_", NorwegianAnalyzer.getDefaultStopSet());
        namedStopWords.put("_persian_", PersianAnalyzer.getDefaultStopSet());
        namedStopWords.put("_portuguese_", PortugueseAnalyzer.getDefaultStopSet());
        namedStopWords.put("_romanian_", RomanianAnalyzer.getDefaultStopSet());
        namedStopWords.put("_russian_", RussianAnalyzer.getDefaultStopSet());
        namedStopWords.put("_sorani_", SoraniAnalyzer.getDefaultStopSet());
        namedStopWords.put("_spanish_", SpanishAnalyzer.getDefaultStopSet());
        namedStopWords.put("_swedish_", SwedishAnalyzer.getDefaultStopSet());
        namedStopWords.put("_thai_", ThaiAnalyzer.getDefaultStopSet());
        namedStopWords.put("_turkish_", TurkishAnalyzer.getDefaultStopSet());

        NAMED_STOP_WORDS = unmodifiableMap(namedStopWords);
    }

    public static CharArraySet parseWords(
        Environment env,
        Settings settings,
        String name,
        CharArraySet defaultWords,
        Map<String, Set<?>> namedWords,
        boolean ignoreCase
    ) {
        String value = settings.get(name);
        if (value != null) {
            if ("_none_".equals(value)) {
                return CharArraySet.EMPTY_SET;
            } else {
                return resolveNamedWords(settings.getAsList(name), namedWords, ignoreCase);
            }
        }
        List<String> pathLoadedWords = parseWordList(env, settings, name, s -> s);
        if (pathLoadedWords != null) {
            return resolveNamedWords(pathLoadedWords, namedWords, ignoreCase);
        }
        return defaultWords;
    }

    public static CharArraySet parseCommonWords(Environment env, Settings settings, CharArraySet defaultCommonWords, boolean ignoreCase) {
        return parseWords(env, settings, "common_words", defaultCommonWords, NAMED_STOP_WORDS, ignoreCase);
    }

    public static CharArraySet parseArticles(Environment env, Settings settings) {
        boolean articlesCase = settings.getAsBoolean("articles_case", false);
        return parseWords(env, settings, "articles", null, null, articlesCase);
    }

    public static CharArraySet parseStopWords(Environment env, Settings settings, CharArraySet defaultStopWords) {
        boolean stopwordsCase = settings.getAsBoolean("stopwords_case", false);
        return parseStopWords(env, settings, defaultStopWords, stopwordsCase);
    }

    public static CharArraySet parseStopWords(Environment env, Settings settings, CharArraySet defaultStopWords, boolean ignoreCase) {
        return parseWords(env, settings, "stopwords", defaultStopWords, NAMED_STOP_WORDS, ignoreCase);
    }

    private static CharArraySet resolveNamedWords(Collection<String> words, Map<String, Set<?>> namedWords, boolean ignoreCase) {
        if (namedWords == null) {
            return new CharArraySet(words, ignoreCase);
        }
        CharArraySet setWords = new CharArraySet(words.size(), ignoreCase);
        for (String word : words) {
            if (namedWords.containsKey(word)) {
                setWords.addAll(namedWords.get(word));
            } else {
                setWords.add(word);
            }
        }
        return setWords;
    }

    public static CharArraySet getWordSet(Environment env, Settings settings, String settingsPrefix) {
        List<String> wordList = parseWordList(env, settings, settingsPrefix, s -> s);
        if (wordList == null) {
            return null;
        }
        boolean ignoreCase = settings.getAsBoolean(settingsPrefix + "_case", false);
        return new CharArraySet(wordList, ignoreCase);
    }

    public static <T> List<T> parseWordList(Environment env, Settings settings, String settingPrefix, CustomMappingRuleParser<T> parser) {
        return parseWordList(env, settings, settingPrefix + "_path", settingPrefix, parser);
    }

    public static <T> List<T> parseWordList(
        Environment env,
        Settings settings,
        String settingPrefix,
        CustomMappingRuleParser<T> parser,
        boolean removeComments
    ) {
        return parseWordList(env, settings, settingPrefix + "_path", settingPrefix, parser, removeComments);
    }

    /**
     * Parses a list of words from the specified settings or from a file, with the given parser.
     *
     * @throws IllegalArgumentException
     *          If the word list cannot be found at either key.
     * @throws RuntimeException
     *          If there is error parsing the words
     */
    public static <T> List<T> parseWordList(
        Environment env,
        Settings settings,
        String settingPath,
        String settingList,
        CustomMappingRuleParser<T> parser
    ) {
        return parseWordList(env, settings, settingPath, settingList, parser, true);
    }

    public static <T> List<T> parseWordList(
        Environment env,
        Settings settings,
        String settingPath,
        String settingList,
        CustomMappingRuleParser<T> parser,
        boolean removeComments
    ) {
        List<String> words = getWordList(env, settings, settingPath, settingList);
        if (words == null) {
            return null;
        }
        List<T> rules = new ArrayList<>();
        int lineNum = 0;
        for (String word : words) {
            lineNum++;
            if (removeComments == false || word.startsWith("#") == false) {
                try {
                    rules.add(parser.apply(word));
                } catch (RuntimeException ex) {
                    String wordListPath = settings.get(settingPath, null);
                    if (wordListPath == null || isUnderConfig(env, wordListPath)) {
                        throw new RuntimeException("Line [" + lineNum + "]: " + ex.getMessage());
                    } else {
                        LOGGER.error("Line [{}]: {}", lineNum, ex);
                        throw new RuntimeException("Line [" + lineNum + "]: " + "Invalid rule");
                    }
                }
            }
        }
        return rules;
    }

    /**
     * Fetches a list of words from the specified settings file. The list should either be available at the key
     * specified by <code>settingList</code> or in a file specified by <code>settingPath</code>.
     *
     * @throws IllegalArgumentException
     *          If the word list cannot be found at either key.
     */
    private static List<String> getWordList(Environment env, Settings settings, String settingPath, String settingList) {
        String wordListPath = settings.get(settingPath, null);

        if (wordListPath == null) {
            return settings.getAsList(settingList, null);
        }

        final Path path = resolveAnalyzerPath(env, wordListPath);

        try {
            return loadWordList(path);
        } catch (CharacterCodingException ex) {
            String message = String.format(
                Locale.ROOT,
                "Unsupported character encoding detected while reading %s: files must be UTF-8 encoded",
                settingPath
            );
            LOGGER.error("{}: from file: {}, exception is: {}", message, path.toString(), ex);
            throw new IllegalArgumentException(message);
        } catch (IOException ioe) {
            String message = String.format(Locale.ROOT, "IOException while reading %s: file not readable", settingPath);
            LOGGER.error("{}, from file: {}, exception is: {}", message, path.toString(), ioe);
            throw new IllegalArgumentException(message);
        }
    }

    private static List<String> loadWordList(Path path) throws IOException {
        final List<String> result = new ArrayList<>();
        try (BufferedReader br = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            String word;
            while ((word = br.readLine()) != null) {
                if (Strings.hasText(word) == false) {
                    continue;
                }
                result.add(word.trim());
            }
        }
        return result;
    }

    /**
     * @return null If no settings set for "settingsPrefix" then return <code>null</code>.
     * @throws IllegalArgumentException
     *          If the Reader can not be instantiated.
     */
    public static Reader getReaderFromFile(Environment env, Settings settings, String settingPrefix) {
        String filePath = settings.get(settingPrefix, null);

        if (filePath == null) {
            return null;
        }
        final Path path = resolveAnalyzerPath(env, filePath);
        try {
            return Files.newBufferedReader(path, StandardCharsets.UTF_8);
        } catch (CharacterCodingException ex) {
            String message = String.format(
                Locale.ROOT,
                "Unsupported character encoding detected while reading %s_path: files must be UTF-8 encoded",
                settingPrefix
            );
            LOGGER.error("{}: from file: {}, exception is: {}", message, path.toString(), ex);
            throw new IllegalArgumentException(message);
        } catch (IOException ioe) {
            String message = String.format(Locale.ROOT, "IOException while reading %s_path: file not readable", settingPrefix);
            LOGGER.error("{}, from file: {}, exception is: {}", message, path.toString(), ioe);
            throw new IllegalArgumentException(message);
        }
    }

    public static Path resolveAnalyzerPath(Environment env, String wordListPath) {
        return env.configDir().resolve(wordListPath).normalize();
    }

    private static boolean isUnderConfig(Environment env, String wordListPath) {
        try {
            final Path path = env.configDir().resolve(wordListPath).normalize();
            return path.startsWith(env.configDir().toAbsolutePath());
        } catch (Exception ex) {
            return false;
        }
    }
}
