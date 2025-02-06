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

package org.opensearch.index.similarity;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.util.BytesRef;
import org.opensearch.Version;
import org.opensearch.common.TriFunction;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.AbstractIndexComponent;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.lucene.similarity.LegacyBM25Similarity;
import org.opensearch.script.ScriptService;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Service for similarity computations
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class SimilarityService extends AbstractIndexComponent {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(SimilarityService.class);
    public static final String DEFAULT_SIMILARITY = "BM25";
    private static final String CLASSIC_SIMILARITY = "classic";
    private static final Map<String, Function<Version, Supplier<Similarity>>> DEFAULTS;
    public static final Map<String, TriFunction<Settings, Version, ScriptService, Similarity>> BUILT_IN;
    static {
        Map<String, Function<Version, Supplier<Similarity>>> defaults = new HashMap<>();
        defaults.put(CLASSIC_SIMILARITY, version -> {
            return () -> {
                throw new IllegalArgumentException(
                    "The [classic] similarity may not be used anymore. Please use the [BM25] "
                        + "similarity or build a custom [scripted] similarity instead."
                );
            };
        });
        defaults.put("BM25", version -> {
            final LegacyBM25Similarity similarity = SimilarityProviders.createBM25Similarity(Settings.EMPTY, version);
            return () -> similarity;
        });
        defaults.put("boolean", version -> {
            final Similarity similarity = new BooleanSimilarity();
            return () -> similarity;
        });

        Map<String, TriFunction<Settings, Version, ScriptService, Similarity>> builtIn = new HashMap<>();
        builtIn.put(CLASSIC_SIMILARITY, (settings, version, script) -> {
            throw new IllegalArgumentException(
                "The [classic] similarity may not be used anymore. Please use the [BM25] "
                    + "similarity or build a custom [scripted] similarity instead."
            );
        });
        builtIn.put("BM25", (settings, version, scriptService) -> SimilarityProviders.createBM25Similarity(settings, version));
        builtIn.put("boolean", (settings, version, scriptService) -> SimilarityProviders.createBooleanSimilarity(settings, version));
        builtIn.put("DFR", (settings, version, scriptService) -> SimilarityProviders.createDfrSimilarity(settings, version));
        builtIn.put("IB", (settings, version, scriptService) -> SimilarityProviders.createIBSimilarity(settings, version));
        builtIn.put(
            "LMDirichlet",
            (settings, version, scriptService) -> SimilarityProviders.createLMDirichletSimilarity(settings, version)
        );
        builtIn.put(
            "LMJelinekMercer",
            (settings, version, scriptService) -> SimilarityProviders.createLMJelinekMercerSimilarity(settings, version)
        );
        builtIn.put("DFI", (settings, version, scriptService) -> SimilarityProviders.createDfiSimilarity(settings, version));
        builtIn.put("scripted", new ScriptedSimilarityProvider());
        DEFAULTS = Collections.unmodifiableMap(defaults);
        BUILT_IN = Collections.unmodifiableMap(builtIn);
    }

    private final Similarity defaultSimilarity;
    private final Map<String, Supplier<Similarity>> similarities;

    public SimilarityService(
        IndexSettings indexSettings,
        ScriptService scriptService,
        Map<String, TriFunction<Settings, Version, ScriptService, Similarity>> similarities
    ) {
        super(indexSettings);
        Map<String, Supplier<Similarity>> providers = new HashMap<>(similarities.size());
        Map<String, Settings> similaritySettings = this.indexSettings.getSettings().getGroups(IndexModule.SIMILARITY_SETTINGS_PREFIX);

        for (Map.Entry<String, Settings> entry : similaritySettings.entrySet()) {
            String name = entry.getKey();
            if (BUILT_IN.containsKey(name)) {
                throw new IllegalArgumentException("Cannot redefine built-in Similarity [" + name + "]");
            }
            Settings providerSettings = entry.getValue();
            String typeName = providerSettings.get("type");
            if (typeName == null) {
                throw new IllegalArgumentException("Similarity [" + name + "] must have an associated type");
            } else if ((similarities.containsKey(typeName) || BUILT_IN.containsKey(typeName)) == false) {
                throw new IllegalArgumentException("Unknown Similarity type [" + typeName + "] for [" + name + "]");
            }
            TriFunction<Settings, Version, ScriptService, Similarity> defaultFactory = BUILT_IN.get(typeName);
            TriFunction<Settings, Version, ScriptService, Similarity> factory = similarities.getOrDefault(typeName, defaultFactory);
            Similarity similarity = factory.apply(providerSettings, indexSettings.getIndexVersionCreated(), scriptService);
            validateSimilarity(indexSettings.getIndexVersionCreated(), similarity);
            if (BUILT_IN.containsKey(typeName) == false || "scripted".equals(typeName)) {
                // We don't trust custom similarities
                similarity = new NonNegativeScoresSimilarity(similarity);
            }
            final Similarity similarityF = similarity; // like similarity but final
            providers.put(name, () -> similarityF);
        }
        for (Map.Entry<String, Function<Version, Supplier<Similarity>>> entry : DEFAULTS.entrySet()) {
            providers.put(entry.getKey(), entry.getValue().apply(indexSettings.getIndexVersionCreated()));
        }
        this.similarities = providers;
        defaultSimilarity = (providers.get("default") != null)
            ? providers.get("default").get()
            : providers.get(SimilarityService.DEFAULT_SIMILARITY).get();
        if (providers.get("base") != null) {
            deprecationLogger.deprecate(
                "base_similarity_ignored",
                "The [base] similarity is ignored since query normalization and coords have been removed"
            );
        }
    }

    public Similarity similarity(MapperService mapperService) {
        // TODO we can maybe factor out MapperService here entirely by introducing an interface for the lookup?
        return (mapperService != null) ? new PerFieldSimilarity(defaultSimilarity, mapperService) : defaultSimilarity;
    }

    public SimilarityProvider getSimilarity(String name) {
        Supplier<Similarity> sim = similarities.get(name);
        if (sim == null) {
            return null;
        }
        return new SimilarityProvider(name, sim.get());
    }

    // for testing
    Similarity getDefaultSimilarity() {
        return defaultSimilarity;
    }

    /**
     * Similarity per field
     *
     * @opensearch.internal
     */
    static class PerFieldSimilarity extends PerFieldSimilarityWrapper {

        private final Similarity defaultSimilarity;
        private final MapperService mapperService;

        PerFieldSimilarity(Similarity defaultSimilarity, MapperService mapperService) {
            super();
            this.defaultSimilarity = defaultSimilarity;
            this.mapperService = mapperService;
        }

        @Override
        public Similarity get(String name) {
            MappedFieldType fieldType = mapperService.fieldType(name);
            return (fieldType != null && fieldType.getTextSearchInfo().getSimilarity() != null)
                ? fieldType.getTextSearchInfo().getSimilarity().get()
                : defaultSimilarity;
        }
    }

    static void validateSimilarity(Version indexCreatedVersion, Similarity similarity) {
        validateScoresArePositive(indexCreatedVersion, similarity);
        validateScoresDoNotDecreaseWithFreq(indexCreatedVersion, similarity);
        validateScoresDoNotIncreaseWithNorm(indexCreatedVersion, similarity);
    }

    private static void validateScoresArePositive(Version indexCreatedVersion, Similarity similarity) {
        CollectionStatistics collectionStats = new CollectionStatistics("some_field", 1200, 1100, 3000, 2000);
        TermStatistics termStats = new TermStatistics(new BytesRef("some_value"), 100, 130);
        SimScorer scorer = similarity.scorer(2f, collectionStats, termStats);
        FieldInvertState state = new FieldInvertState(
            indexCreatedVersion.luceneVersion.major,
            "some_field",
            IndexOptions.DOCS_AND_FREQS,
            20,
            20,
            0,
            50,
            10,
            3
        ); // length = 20, no overlap
        final long norm = similarity.computeNorm(state);
        for (int freq = 1; freq <= 10; ++freq) {
            float score = scorer.score(freq, norm);
            if (score < 0) {
                fail("Similarities should not return negative scores:\n" + scorer.explain(Explanation.match(freq, "term freq"), norm));
                break;
            }
        }
    }

    private static void validateScoresDoNotDecreaseWithFreq(Version indexCreatedVersion, Similarity similarity) {
        CollectionStatistics collectionStats = new CollectionStatistics("some_field", 1200, 1100, 3000, 2000);
        TermStatistics termStats = new TermStatistics(new BytesRef("some_value"), 100, 130);
        SimScorer scorer = similarity.scorer(2f, collectionStats, termStats);
        FieldInvertState state = new FieldInvertState(
            indexCreatedVersion.luceneVersion.major,
            "some_field",
            IndexOptions.DOCS_AND_FREQS,
            20,
            20,
            0,
            50,
            10,
            3
        ); // length = 20, no overlap
        final long norm = similarity.computeNorm(state);
        float previousScore = 0;
        for (int freq = 1; freq <= 10; ++freq) {
            float score = scorer.score(freq, norm);
            if (score < previousScore) {
                fail(
                    "Similarity scores should not decrease when term frequency increases:\n"
                        + scorer.explain(Explanation.match(freq - 1, "term freq"), norm)
                        + "\n"
                        + scorer.explain(Explanation.match(freq, "term freq"), norm)
                );
                break;
            }
            previousScore = score;
        }
    }

    private static void validateScoresDoNotIncreaseWithNorm(Version indexCreatedVersion, Similarity similarity) {
        CollectionStatistics collectionStats = new CollectionStatistics("some_field", 1200, 1100, 3000, 2000);
        TermStatistics termStats = new TermStatistics(new BytesRef("some_value"), 100, 130);
        SimScorer scorer = similarity.scorer(2f, collectionStats, termStats);

        long previousNorm = 0;
        float previousScore = Float.MAX_VALUE;
        for (int length = 1; length <= 10; ++length) {
            FieldInvertState state = new FieldInvertState(
                indexCreatedVersion.luceneVersion.major,
                "some_field",
                IndexOptions.DOCS_AND_FREQS,
                length,
                length,
                0,
                50,
                10,
                3
            ); // length = 20, no overlap
            final long norm = similarity.computeNorm(state);
            if (Long.compareUnsigned(previousNorm, norm) > 0) {
                // esoteric similarity, skip this check
                break;
            }
            float score = scorer.score(1, norm);
            if (score > previousScore) {
                fail(
                    "Similarity scores should not increase when norm increases:\n"
                        + scorer.explain(Explanation.match(1, "term freq"), norm - 1)
                        + "\n"
                        + scorer.explain(Explanation.match(1, "term freq"), norm)
                );
                break;
            }
            previousScore = score;
            previousNorm = norm;
        }
    }

    private static void fail(String message) {
        throw new IllegalArgumentException(message);
    }

}
