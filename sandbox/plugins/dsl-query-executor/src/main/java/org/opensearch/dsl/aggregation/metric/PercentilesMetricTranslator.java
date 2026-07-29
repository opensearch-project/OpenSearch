/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.metric;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.opensearch.dsl.aggregation.LiteralColumnAllocator;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.opensearch.search.aggregations.metrics.PercentilesConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Translator for the percentiles aggregation. One request node fans out to one
 * {@code PERCENTILE_APPROX(field, percent)} call per requested percent, bound by the
 * DataFusion backend to {@code approx_percentile_cont} (t-digest, so values are
 * approximate like legacy). Honors {@code missing}, {@code format}, and tdigest
 * {@code compression} (as the engine's centroids arg, 3-arg {@code PERCENTILE_APPROX_N}).
 * HDR is unsupported and fails conversion — there is no classic-path fallback.
 */
public class PercentilesMetricTranslator implements MetricTranslator<PercentilesAggregationBuilder> {

    /** Creates a percentiles metric translator. */
    public PercentilesMetricTranslator() {}

    @Override
    public Class<PercentilesAggregationBuilder> getAggregationType() {
        return PercentilesAggregationBuilder.class;
    }

    @Override
    public List<AggregateCall> toAggregateCalls(PercentilesAggregationBuilder agg, RelDataType rowType) throws ConversionException {
        throw new ConversionException("percentiles requires literal column support; use the LiteralColumnAllocator variant");
    }

    @Override
    public List<AggregateCall> toAggregateCalls(PercentilesAggregationBuilder agg, RelDataType rowType, LiteralColumnAllocator literals)
        throws ConversionException {
        validate(agg);
        RelDataTypeField field = MetricTranslator.resolveNumericField(rowType, agg.field(), agg.getType());

        int inputColumn = field.getIndex();
        if (agg.missing() != null) {
            inputColumn = literals.coalescedColumnFor(field.getIndex(), MetricTranslator.missingValue(agg.missing(), agg.getName()));
        }

        Long centroidCount = centroids(agg);
        Integer centroidsColumn = centroidCount == null ? null : literals.integerColumnFor(centroidCount);

        // Declared with the field's type; both percentile ops infer ARG0 forced nullable, and the
        // metadata builder normalizes the declared type to nullable to match (it owns the factory).
        List<AggregateCall> calls = new ArrayList<>(agg.percentiles().length);
        for (double percent : agg.percentiles()) {
            int percentColumn = literals.columnFor(percent);
            calls.add(
                AggregateCall.create(
                    centroidsColumn == null ? PercentileApproxFunction.INSTANCE : PercentileApproxFunction.INSTANCE_N,
                    false,
                    true,
                    false,
                    centroidsColumn == null ? List.of(inputColumn, percentColumn) : List.of(inputColumn, percentColumn, centroidsColumn),
                    -1,
                    RelCollations.EMPTY,
                    field.getType(),
                    columnName(agg.getName(), percent)
                )
            );
        }
        return calls;
    }

    /** Vanilla's default t-digest compression; also the engine's default centroid count. */
    private static final double DEFAULT_COMPRESSION = 100.0;

    /**
     * Centroids for a non-default tdigest compression, or {@code null} for the engine default.
     * Config presence is not the signal — the request parser injects {@code TDigest(100)} when
     * the JSON names no method — so only a non-default compression pins the value in the plan.
     */
    private static Long centroids(PercentilesAggregationBuilder agg) throws ConversionException {
        if (agg.percentilesConfig() instanceof PercentilesConfig.TDigest tdigest && tdigest.getCompression() != DEFAULT_COMPRESSION) {
            long centroidCount = Math.round(tdigest.getCompression());
            if (centroidCount < 1) {
                throw new ConversionException(
                    "percentiles aggregation ["
                        + agg.getName()
                        + "] has compression ["
                        + tdigest.getCompression()
                        + "]; the analytics path requires at least 1"
                );
            }
            return centroidCount;
        }
        return null;
    }

    @Override
    public List<String> getAggregateFieldNames(PercentilesAggregationBuilder agg) {
        List<String> names = new ArrayList<>(agg.percentiles().length);
        for (double percent : agg.percentiles()) {
            names.add(columnName(agg.getName(), percent));
        }
        return names;
    }

    @Override
    public InternalAggregation toInternalAggregation(PercentilesAggregationBuilder agg, Map<String, Object> values) {
        double[] percents = agg.percentiles();
        double[] results = new double[percents.length];
        for (int i = 0; i < percents.length; i++) {
            Object cell = values == null ? null : values.get(columnName(agg.getName(), percents[i]));
            results[i] = cell == null ? Double.NaN : ((Number) cell).doubleValue();
        }
        return new InternalDslPercentiles(agg.getName(), percents, results, agg.keyed(), MetricTranslator.parseFormat(agg.format()));
    }

    private static void validate(PercentilesAggregationBuilder agg) throws ConversionException {
        if (agg.percentiles() == null || agg.percentiles().length == 0) {
            throw new ConversionException("percentiles aggregation [" + agg.getName() + "] has no percents");
        }
        PercentilesConfig config = agg.percentilesConfig();
        if (config instanceof PercentilesConfig.Hdr) {
            throw new ConversionException("HDR percentiles method is not supported on the analytics path");
        }
        MetricTranslator.validateFormat(agg.format(), agg.getName());
    }

    /** Output column name for one percent, e.g. {@code lat_p50_0} for percent 50.0. */
    static String columnName(String aggName, double percent) {
        return aggName + "_p" + String.valueOf(percent).replace('.', '_').replace('-', '_');
    }
}
