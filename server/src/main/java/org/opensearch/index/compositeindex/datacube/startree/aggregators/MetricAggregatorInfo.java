/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.aggregators;

import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.mapper.FieldValueConverter;

import java.util.Comparator;
import java.util.Objects;

/**
 * Builds aggregation function and doc values field pair to support various aggregations
 *
 * @opensearch.experimental
 */
public class MetricAggregatorInfo implements Comparable<MetricAggregatorInfo> {

    public static final String DELIMITER = "_";
    private final String metric;
    private final String starFieldName;
    private final MetricStat metricStat;
    private final String field;
    private final ValueAggregator valueAggregators;
    private final FieldValueConverter fieldValueConverter;

    /**
     * Constructor for MetricAggregatorInfo
     */
    public MetricAggregatorInfo(MetricStat metricStat, String field, String starFieldName, FieldValueConverter fieldValueConverter) {
        this.metricStat = metricStat;
        this.fieldValueConverter = fieldValueConverter;
        this.valueAggregators = ValueAggregatorFactory.getValueAggregator(metricStat, this.fieldValueConverter);
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
    public FieldValueConverter getNumericFieldConverter() {
        return fieldValueConverter;
    }

    /**
     * @return field name with metric type and field
     */
    public String toFieldName() {
        return toFieldName(starFieldName, field, metricStat.getTypeName());

    }

    /**
     * @return field name with star-tree field name metric type and field
     */
    public static String toFieldName(String starFieldName, String field, String metricName) {
        return starFieldName + DELIMITER + field + DELIMITER + metricName;
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
            return metricStat.equals(anotherPair.metricStat) && field.equals(anotherPair.field);
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
