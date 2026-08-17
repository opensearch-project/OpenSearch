/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.pipeline;

import org.opensearch.search.aggregations.PipelineAggregationBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * Registry of pipeline aggregation translators, keyed by PipelineAggregationBuilder class.
 * Mirrors {@link org.opensearch.dsl.aggregation.AggregationRegistry}.
 */
public class PipelineRegistry {

    private final Map<Class<? extends PipelineAggregationBuilder>, PipelineTranslator<?>> translators = new HashMap<>();

    private PipelineRegistry() {}

    /** Creates a registry with all supported pipeline translators. */
    public static PipelineRegistry create() {
        PipelineRegistry registry = new PipelineRegistry();
        registry.register(new AvgBucketTranslator());
        return registry;
    }

    /**
     * Registers a translator.
     *
     * @param translator the translator to register
     */
    public void register(PipelineTranslator<?> translator) {
        translators.put(translator.getBuilderClass(), translator);
    }

    /**
     * Returns the translator for the given builder class, or null when the pipeline
     * aggregation type is not supported.
     *
     * @param builderClass the pipeline aggregation builder class
     * @return the translator, or null
     */
    @SuppressWarnings("unchecked")
    public <T extends PipelineAggregationBuilder> PipelineTranslator<T> get(Class<? extends PipelineAggregationBuilder> builderClass) {
        return (PipelineTranslator<T>) translators.get(builderClass);
    }
}
