/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.pipeline;

import org.apache.calcite.rel.RelNode;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.PipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.BucketMetricsPipelineAggregationBuilder;

/**
 * Translates a pipeline aggregation DSL builder into a Calcite RelNode
 * transformation applied on top of the base LogicalAggregate, and converts
 * execution results back to {@link InternalAggregation} for response building.
 *
 * @param <T> the concrete PipelineAggregationBuilder type
 */
public interface PipelineTranslator<T extends PipelineAggregationBuilder> {

    /** Classifies how a pipeline aggregation interacts with the base plan. */
    enum Type {
        /** Modifies the base plan in-place (e.g., bucket_sort adds sort/limit). */
        PARENT,
        /** Produces an independent result plan from the base (e.g., avg_bucket, stats_bucket). */
        SIBLING
    }

    /** Returns the concrete PipelineAggregationBuilder class this translator handles. */
    Class<T> getBuilderClass();

    /** Returns whether this is a parent or sibling pipeline translator. Defaults to SIBLING. */
    Type type();

    /**
     * Translates the pipeline aggregation into a Calcite plan transformation.
     *
     * @param builder the pipeline aggregation builder from DSL
     * @param input   the current RelNode (base LogicalAggregate or output of previous pipeline)
     * @param ctx     conversion context with cluster, RexBuilder, row type
     * @return a new RelNode with the pipeline operation applied
     * @throws ConversionException if translation fails
     */
    RelNode translate(T builder, RelNode input, ConversionContext ctx) throws ConversionException;

    /**
     * Converts a single result row from the pipeline aggregation execution into
     * an {@link InternalAggregation} for response building.
     *
     * @param builder the pipeline aggregation builder from DSL
     * @param row     the result row from executing the pipeline's RelNode
     * @return the InternalAggregation representing the pipeline result
     */
    InternalAggregation toInternalAggregation(T builder, Object[] row);

    /**
     * Resolves the {@link DocValueFormat} from a pipeline aggregation builder's
     * {@code format} parameter. Returns {@link DocValueFormat.Decimal} when a
     * format string is specified, or {@link DocValueFormat#RAW} otherwise.
     *
     * @param builder the pipeline aggregation builder
     * @return the resolved DocValueFormat
     */
    static DocValueFormat resolveFormat(PipelineAggregationBuilder builder) {
        if (builder instanceof BucketMetricsPipelineAggregationBuilder) {
            String format = ((BucketMetricsPipelineAggregationBuilder<?>) builder).format();
            if (format != null) {
                return new DocValueFormat.Decimal(format);
            }
        }
        return DocValueFormat.RAW;
    }
}
