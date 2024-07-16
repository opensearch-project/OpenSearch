/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.aggregators;

import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.numerictype.StarTreeNumericType;
import org.opensearch.index.fielddata.IndexNumericFieldData;

import java.util.Comparator;
import java.util.Objects;

/**
 * Builds aggregation function and doc values field pair to support various aggregations
 * @opensearch.experimental
 */
public class MetricAggregatorInfo implements Comparable<MetricAggregatorInfo> {

    public static final String DELIMITER = "_";
    private final String metric;
    private final String starFieldName;
    private final MetricStat metricStat;
    private final String field;
    private final ValueAggregator valueAggregators;
    private final StarTreeNumericType starTreeNumericType;

    /**
     * Constructor for MetricAggregatorInfo
     */
    public MetricAggregatorInfo(MetricStat metricStat, String field, String starFieldName, IndexNumericFieldData.NumericType numericType) {
        this.metricStat = metricStat;
        this.valueAggregators = ValueAggregatorFactory.getValueAggregator(metricStat);
        this.starTreeNumericType = StarTreeNumericType.fromNumericType(numericType);
        this.field = field;
        this.starFieldName = starFieldName;
        this.metric = toFieldName();
    }

    /**
     * @return metric type
     */
    public MetricStat getMetricStat() {
        return metricStat;
    }

    /**
     * @return field Name
     */
    public String getField() {
        return field;
    }

    /**
     * @return the metric stat name
     */
    public String getMetric() {
        return metric;
    }

    /**
     * @return aggregator for the field value
     */
    public ValueAggregator getValueAggregators() {
        return valueAggregators;
    }

    /**
     * @return star tree aggregated value type
     */
    public StarTreeNumericType getAggregatedValueType() {
        return starTreeNumericType;
    }

    /**
     * @return field name with metric type and field
     */
    public String toFieldName() {
        return starFieldName + DELIMITER + field + DELIMITER + metricStat.getTypeName();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(toFieldName());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof MetricAggregatorInfo) {
            MetricAggregatorInfo anotherPair = (MetricAggregatorInfo) obj;
            return metricStat == anotherPair.metricStat && field.equals(anotherPair.field);
        }
        return false;
    }

    @Override
    public String toString() {
        return toFieldName();
    }

    @Override
    public int compareTo(MetricAggregatorInfo other) {
        return Comparator.comparing((MetricAggregatorInfo o) -> o.field)
            .thenComparing((MetricAggregatorInfo o) -> o.metricStat)
            .compare(this, other);
    }
}
