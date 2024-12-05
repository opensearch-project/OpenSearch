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

import org.apache.lucene.misc.search.similarity.LegacyBM25Similarity;
import org.apache.lucene.search.similarities.AfterEffect;
import org.apache.lucene.search.similarities.AfterEffectB;
import org.apache.lucene.search.similarities.AfterEffectL;
import org.apache.lucene.search.similarities.BasicModel;
import org.apache.lucene.search.similarities.BasicModelG;
import org.apache.lucene.search.similarities.BasicModelIF;
import org.apache.lucene.search.similarities.BasicModelIn;
import org.apache.lucene.search.similarities.BasicModelIne;
import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.DFISimilarity;
import org.apache.lucene.search.similarities.DFRSimilarity;
import org.apache.lucene.search.similarities.Distribution;
import org.apache.lucene.search.similarities.DistributionLL;
import org.apache.lucene.search.similarities.DistributionSPL;
import org.apache.lucene.search.similarities.IBSimilarity;
import org.apache.lucene.search.similarities.Independence;
import org.apache.lucene.search.similarities.IndependenceChiSquared;
import org.apache.lucene.search.similarities.IndependenceSaturated;
import org.apache.lucene.search.similarities.IndependenceStandardized;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.Lambda;
import org.apache.lucene.search.similarities.LambdaDF;
import org.apache.lucene.search.similarities.LambdaTTF;
import org.apache.lucene.search.similarities.Normalization;
import org.apache.lucene.search.similarities.NormalizationH1;
import org.apache.lucene.search.similarities.NormalizationH2;
import org.apache.lucene.search.similarities.NormalizationH3;
import org.apache.lucene.search.similarities.NormalizationZ;
import org.opensearch.Version;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.settings.Settings;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.unmodifiableMap;

/**
 * A provider for similarity computations
 *
 * @opensearch.internal
 */
final class SimilarityProviders {

    private SimilarityProviders() {} // no instantiation

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(SimilarityProviders.class);
    static final String DISCOUNT_OVERLAPS = "discount_overlaps";

    private static final Map<String, BasicModel> BASIC_MODELS;
    private static final Map<String, String> LEGACY_BASIC_MODELS;
    private static final Map<String, AfterEffect> AFTER_EFFECTS;
    private static final Map<String, String> LEGACY_AFTER_EFFECTS;

    static {
        Map<String, BasicModel> models = new HashMap<>();
        models.put("g", new BasicModelG());
        models.put("if", new BasicModelIF());
        models.put("in", new BasicModelIn());
        models.put("ine", new BasicModelIne());
        BASIC_MODELS = unmodifiableMap(models);

        Map<String, String> legacyModels = new HashMap<>();
        // TODO: be and g and both based on the bose-einstein model.
        // Is there a better replacement for d and p which use the binomial model?
        legacyModels.put("be", "g");
        legacyModels.put("d", "ine");
        legacyModels.put("p", "ine");
        LEGACY_BASIC_MODELS = unmodifiableMap(legacyModels);

        Map<String, AfterEffect> effects = new HashMap<>();
        effects.put("b", new AfterEffectB());
        effects.put("l", new AfterEffectL());
        AFTER_EFFECTS = unmodifiableMap(effects);

        Map<String, String> legacyEffects = new HashMap<>();
        // l is simpler than b, so this should be a better replacement for "no"
        legacyEffects.put("no", "l");
        LEGACY_AFTER_EFFECTS = unmodifiableMap(legacyEffects);
    }

    private static final Map<String, Independence> INDEPENDENCE_MEASURES;
    static {
        Map<String, Independence> measures = new HashMap<>();
        measures.put("standardized", new IndependenceStandardized());
        measures.put("saturated", new IndependenceSaturated());
        measures.put("chisquared", new IndependenceChiSquared());
        INDEPENDENCE_MEASURES = unmodifiableMap(measures);
    }

    private static final Map<String, Distribution> DISTRIBUTIONS;
    private static final Map<String, Lambda> LAMBDAS;

    static {
        Map<String, Distribution> distributions = new HashMap<>();
        distributions.put("ll", new DistributionLL());
        distributions.put("spl", new DistributionSPL());
        DISTRIBUTIONS = unmodifiableMap(distributions);

        Map<String, Lambda> lamdas = new HashMap<>();
        lamdas.put("df", new LambdaDF());
        lamdas.put("ttf", new LambdaTTF());
        LAMBDAS = unmodifiableMap(lamdas);
    }

    /**
     * Parses the given Settings and creates the appropriate {@link BasicModel}
     *
     * @param settings Settings to parse
     * @return {@link BasicModel} referred to in the Settings
     */
    private static BasicModel parseBasicModel(Version indexCreatedVersion, Settings settings) {
        String basicModel = settings.get("basic_model");
        BasicModel model = BASIC_MODELS.get(basicModel);

        if (model == null) {
            String replacement = LEGACY_BASIC_MODELS.get(basicModel);
            if (replacement != null) {
                throw new IllegalArgumentException(
                    "Basic model [" + basicModel + "] isn't supported anymore, " + "please use another model."
                );
            }
        }

        if (model == null) {
            throw new IllegalArgumentException("Unsupported BasicModel [" + basicModel + "], expected one of " + BASIC_MODELS.keySet());
        }
        return model;
    }

    /**
     * Parses the given Settings and creates the appropriate {@link AfterEffect}
     *
     * @param settings Settings to parse
     * @return {@link AfterEffect} referred to in the Settings
     */
    private static AfterEffect parseAfterEffect(Version indexCreatedVersion, Settings settings) {
        String afterEffect = settings.get("after_effect");
        AfterEffect effect = AFTER_EFFECTS.get(afterEffect);

        if (effect == null) {
            String replacement = LEGACY_AFTER_EFFECTS.get(afterEffect);
            if (replacement != null) {
                throw new IllegalArgumentException(
                    "After effect [" + afterEffect + "] isn't supported anymore, please use another effect."
                );
            }
        }

        if (effect == null) {
            throw new IllegalArgumentException("Unsupported AfterEffect [" + afterEffect + "], expected one of " + AFTER_EFFECTS.keySet());
        }
        return effect;
    }

    /**
     * Parses the given Settings and creates the appropriate {@link Normalization}
     *
     * @param settings Settings to parse
     * @return {@link Normalization} referred to in the Settings
     */
    private static Normalization parseNormalization(Settings settings) {
        String normalization = settings.get("normalization");

        if ("no".equals(normalization)) {
            return new Normalization.NoNormalization();
        } else if ("h1".equals(normalization)) {
            float c = settings.getAsFloat("normalization.h1.c", 1f);
            return new NormalizationH1(c);
        } else if ("h2".equals(normalization)) {
            float c = settings.getAsFloat("normalization.h2.c", 1f);
            return new NormalizationH2(c);
        } else if ("h3".equals(normalization)) {
            float c = settings.getAsFloat("normalization.h3.c", 800f);
            return new NormalizationH3(c);
        } else if ("z".equals(normalization)) {
            float z = settings.getAsFloat("normalization.z.z", 0.30f);
            return new NormalizationZ(z);
        } else {
            throw new IllegalArgumentException("Unsupported Normalization [" + normalization + "]");
        }
    }

    private static Independence parseIndependence(Settings settings) {
        String name = settings.get("independence_measure");
        Independence measure = INDEPENDENCE_MEASURES.get(name);
        if (measure == null) {
            throw new IllegalArgumentException(
                "Unsupported IndependenceMeasure [" + name + "], expected one of " + INDEPENDENCE_MEASURES.keySet()
            );
        }
        return measure;
    }

    /**
     * Parses the given Settings and creates the appropriate {@link Distribution}
     *
     * @param settings Settings to parse
     * @return {@link Normalization} referred to in the Settings
     */
    private static Distribution parseDistribution(Settings settings) {
        String rawDistribution = settings.get("distribution");
        Distribution distribution = DISTRIBUTIONS.get(rawDistribution);
        if (distribution == null) {
            throw new IllegalArgumentException("Unsupported Distribution [" + rawDistribution + "]");
        }
        return distribution;
    }

    /**
     * Parses the given Settings and creates the appropriate {@link Lambda}
     *
     * @param settings Settings to parse
     * @return {@link Normalization} referred to in the Settings
     */
    private static Lambda parseLambda(Settings settings) {
        String rawLambda = settings.get("lambda");
        Lambda lambda = LAMBDAS.get(rawLambda);
        if (lambda == null) {
            throw new IllegalArgumentException("Unsupported Lambda [" + rawLambda + "]");
        }
        return lambda;
    }

    static void assertSettingsIsSubsetOf(String type, Version version, Settings settings, String... supportedSettings) {
        Set<String> unknownSettings = new HashSet<>(settings.keySet());
        unknownSettings.removeAll(Arrays.asList(supportedSettings));
        unknownSettings.remove("type"); // used to figure out which sim this is
        if (unknownSettings.isEmpty() == false) {
            throw new IllegalArgumentException("Unknown settings for similarity of type [" + type + "]: " + unknownSettings);
        }
    }

    public static LegacyBM25Similarity createBM25Similarity(Settings settings, Version indexCreatedVersion) {
        assertSettingsIsSubsetOf("BM25", indexCreatedVersion, settings, "k1", "b", DISCOUNT_OVERLAPS);

        float k1 = settings.getAsFloat("k1", 1.2f);
        float b = settings.getAsFloat("b", 0.75f);
        boolean discountOverlaps = settings.getAsBoolean(DISCOUNT_OVERLAPS, true);

        return new LegacyBM25Similarity(k1, b, discountOverlaps);
    }

    public static BooleanSimilarity createBooleanSimilarity(Settings settings, Version indexCreatedVersion) {
        assertSettingsIsSubsetOf("boolean", indexCreatedVersion, settings);
        return new BooleanSimilarity();
    }

    public static ClassicSimilarity createClassicSimilarity(Settings settings, Version indexCreatedVersion) {
        assertSettingsIsSubsetOf("classic", indexCreatedVersion, settings, DISCOUNT_OVERLAPS);

        boolean discountOverlaps = settings.getAsBoolean(DISCOUNT_OVERLAPS, true);

        ClassicSimilarity similarity = new ClassicSimilarity(discountOverlaps);
        return similarity;
    }

    public static DFRSimilarity createDfrSimilarity(Settings settings, Version indexCreatedVersion) {
        assertSettingsIsSubsetOf(
            "DFR",
            indexCreatedVersion,
            settings,
            "basic_model",
            "after_effect",
            "normalization",
            "normalization.h1.c",
            "normalization.h2.c",
            "normalization.h3.c",
            "normalization.z.z"
        );

        return new DFRSimilarity(
            parseBasicModel(indexCreatedVersion, settings),
            parseAfterEffect(indexCreatedVersion, settings),
            parseNormalization(settings)
        );
    }

    public static DFISimilarity createDfiSimilarity(Settings settings, Version indexCreatedVersion) {
        assertSettingsIsSubsetOf("DFI", indexCreatedVersion, settings, "independence_measure");

        return new DFISimilarity(parseIndependence(settings));
    }

    public static IBSimilarity createIBSimilarity(Settings settings, Version indexCreatedVersion) {
        assertSettingsIsSubsetOf(
            "IB",
            indexCreatedVersion,
            settings,
            "distribution",
            "lambda",
            "normalization",
            "normalization.h1.c",
            "normalization.h2.c",
            "normalization.h3.c",
            "normalization.z.z"
        );

        return new IBSimilarity(parseDistribution(settings), parseLambda(settings), parseNormalization(settings));
    }

    public static LMDirichletSimilarity createLMDirichletSimilarity(Settings settings, Version indexCreatedVersion) {
        assertSettingsIsSubsetOf("LMDirichlet", indexCreatedVersion, settings, "mu");

        float mu = settings.getAsFloat("mu", 2000f);
        return new LMDirichletSimilarity(mu);
    }

    public static LMJelinekMercerSimilarity createLMJelinekMercerSimilarity(Settings settings, Version indexCreatedVersion) {
        assertSettingsIsSubsetOf("LMJelinekMercer", indexCreatedVersion, settings, "lambda");

        float lambda = settings.getAsFloat("lambda", 0.1f);
        return new LMJelinekMercerSimilarity(lambda);
    }
}
