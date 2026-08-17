/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.pipeline;

import org.opensearch.dsl.aggregation.AggregationMetadataBuilder;
import org.opensearch.dsl.aggregation.AggregationRegistry;
import org.opensearch.dsl.aggregation.metric.MetricTranslator;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.PipelineAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.support.AggregationPath;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Resolves a sibling pipeline aggregation's {@code buckets_path} against the request's
 * root-level aggregations.
 *
 * <p>Supported form: a single-level {@code "siblingAgg>metric"} path where the sibling is a
 * root-level {@code terms} aggregation and the metric is one of its single-value metric
 * sub-aggregations, or the built-in {@code _count}. Multi-level paths ({@code a>b>metric}),
 * property paths ({@code stats.avg}), and bucket-key selectors ({@code terms[key]}) are
 * rejected.
 */
public final class BucketsPathResolver {

    private BucketsPathResolver() {}

    /**
     * A resolved buckets_path: the sibling bucket aggregation and the plan column
     * holding the referenced per-bucket value.
     *
     * @param sibling the root-level sibling bucket aggregation the path points into
     * @param metricColumn the column name of the referenced metric in the sibling's plan
     */
    public record ResolvedBucketsPath(TermsAggregationBuilder sibling, String metricColumn) {
    }

    /**
     * Resolves the pipeline's buckets_path to a sibling aggregation and metric column.
     *
     * @param pipeline the sibling pipeline aggregation builder
     * @param rootAggs the request's root-level aggregation builders
     * @param registry the aggregation registry for metric/bucket classification
     * @return the resolved sibling and metric column
     * @throws ConversionException if the path is absent, malformed, or references
     *         anything outside the supported form
     */
    public static ResolvedBucketsPath resolve(
        PipelineAggregationBuilder pipeline,
        Collection<AggregationBuilder> rootAggs,
        AggregationRegistry registry
    ) throws ConversionException {
        String[] paths = pipeline.getBucketsPaths();
        if (paths == null || paths.length != 1 || paths[0] == null || paths[0].isEmpty()) {
            throw new ConversionException("pipeline aggregation [" + pipeline.getName() + "] requires exactly one buckets_path");
        }
        String path = paths[0];

        List<AggregationPath.PathElement> elements;
        try {
            elements = AggregationPath.parse(path).getPathElements();
        } catch (IllegalArgumentException e) {
            throw new ConversionException(
                "pipeline aggregation [" + pipeline.getName() + "] has an invalid buckets_path [" + path + "]: " + e.getMessage()
            );
        }
        if (elements.size() != 2) {
            throw new ConversionException(
                "pipeline aggregation ["
                    + pipeline.getName()
                    + "] buckets_path ["
                    + path
                    + "] must be a single-level [sibling>metric] reference"
            );
        }
        AggregationPath.PathElement siblingElement = elements.get(0);
        AggregationPath.PathElement metricElement = elements.get(1);
        if (siblingElement.key != null || metricElement.key != null) {
            throw new ConversionException(
                "pipeline aggregation [" + pipeline.getName() + "] buckets_path [" + path + "] uses an unsupported property or key path"
            );
        }

        AggregationBuilder sibling = findSibling(pipeline, rootAggs, siblingElement.name);
        if ((sibling instanceof TermsAggregationBuilder) == false) {
            throw new ConversionException(
                "pipeline aggregation ["
                    + pipeline.getName()
                    + "] sibling ["
                    + sibling.getName()
                    + "] of type ["
                    + sibling.getType()
                    + "] is not supported; only [terms] siblings are supported"
            );
        }

        String metricColumn = resolveMetricColumn(pipeline, (TermsAggregationBuilder) sibling, metricElement.name, registry);
        return new ResolvedBucketsPath((TermsAggregationBuilder) sibling, metricColumn);
    }

    private static AggregationBuilder findSibling(
        PipelineAggregationBuilder pipeline,
        Collection<AggregationBuilder> rootAggs,
        String siblingName
    ) throws ConversionException {
        for (AggregationBuilder agg : rootAggs) {
            if (agg.getName().equals(siblingName)) {
                return agg;
            }
        }
        throw new ConversionException(
            "pipeline aggregation ["
                + pipeline.getName()
                + "] buckets_path references unknown aggregation ["
                + siblingName
                + "]. Available: "
                + rootAggs.stream().map(AggregationBuilder::getName).collect(Collectors.toList())
        );
    }

    private static String resolveMetricColumn(
        PipelineAggregationBuilder pipeline,
        TermsAggregationBuilder sibling,
        String metricName,
        AggregationRegistry registry
    ) throws ConversionException {
        if (AggregationMetadataBuilder.IMPLICIT_COUNT_NAME.equals(metricName)) {
            return AggregationMetadataBuilder.IMPLICIT_COUNT_NAME;
        }
        for (AggregationBuilder subAgg : sibling.getSubAggregations()) {
            if (subAgg.getName().equals(metricName)) {
                MetricTranslator<AggregationBuilder> metric = registry.getMetric(subAgg.getClass());
                if (metric == null) {
                    throw new ConversionException(
                        "pipeline aggregation ["
                            + pipeline.getName()
                            + "] buckets_path metric ["
                            + metricName
                            + "] must be a single-value metric aggregation"
                    );
                }
                return metric.getAggregateFieldName(subAgg);
            }
        }
        throw new ConversionException(
            "pipeline aggregation ["
                + pipeline.getName()
                + "] buckets_path metric ["
                + metricName
                + "] not found under ["
                + sibling.getName()
                + "]. Available: "
                + sibling.getSubAggregations().stream().map(AggregationBuilder::getName).collect(Collectors.toList())
        );
    }
}
