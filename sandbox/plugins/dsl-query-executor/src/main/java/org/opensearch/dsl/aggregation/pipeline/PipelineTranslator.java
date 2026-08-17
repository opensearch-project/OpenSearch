/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.pipeline;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.PipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.BucketMetricsPipelineAggregationBuilder;

/**
 * Translates a sibling pipeline aggregation into a second-level SQL aggregate call over
 * a sibling bucket aggregation's output, and converts the executed cell back to an
 * {@link InternalAggregation} for response building.
 *
 * <p>All pipeline aggregations targeting the same sibling share one composed plan
 * (see {@link PipelinePlanComposer}); each translator contributes one aggregate call
 * named after its pipeline aggregation, so results map back by column name.
 *
 * @param <T> the concrete PipelineAggregationBuilder type
 */
public interface PipelineTranslator<T extends PipelineAggregationBuilder> {

    /** Returns the concrete PipelineAggregationBuilder class this translator handles. */
    Class<T> getBuilderClass();

    /**
     * Creates the second-level aggregate call over the resolved metric column.
     *
     * @param builder the pipeline aggregation builder from DSL
     * @param inputColumn index of the metric column in the shaped sibling output
     * @param typeFactory type factory for creating the call's return type
     * @return the aggregate call, named after the pipeline aggregation
     */
    AggregateCall createAggregateCall(T builder, int inputColumn, RelDataTypeFactory typeFactory);

    /**
     * Converts the pipeline's single result cell into an {@link InternalAggregation}.
     *
     * @param builder the pipeline aggregation builder from DSL
     * @param cellValue the result cell, or {@code null} when the engine returned SQL NULL
     *        or no row (empty sibling)
     * @return the InternalAggregation representing the pipeline result
     */
    InternalAggregation toInternalAggregation(T builder, Object cellValue);

    /**
     * Resolves the {@link DocValueFormat} from the builder's {@code format} parameter:
     * {@link DocValueFormat.Decimal} when a pattern is set, {@link DocValueFormat#RAW} otherwise.
     *
     * @param builder the pipeline aggregation builder
     * @return the resolved format
     */
    static DocValueFormat resolveFormat(PipelineAggregationBuilder builder) {
        if (builder instanceof BucketMetricsPipelineAggregationBuilder<?> bucketMetrics && bucketMetrics.format() != null) {
            return new DocValueFormat.Decimal(bucketMetrics.format());
        }
        return DocValueFormat.RAW;
    }
}
