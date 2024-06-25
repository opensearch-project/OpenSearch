/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.aggregators;

import org.apache.lucene.search.DocIdSetIterator;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.numerictype.StarTreeNumericType;
import org.opensearch.index.fielddata.IndexNumericFieldData;

import java.util.Comparator;

/**
 * Builds aggregation function and doc values field pair to support various aggregations
 * @opensearch.experimental
 */
public class MetricAggregationDescriptor implements Comparable<MetricAggregationDescriptor> {

    public static final String DELIMITER = "__";
    public static final String STAR = "*";
    public static final MetricAggregationDescriptor COUNT_STAR = new MetricAggregationDescriptor(
        MetricStat.COUNT,
        STAR,
        IndexNumericFieldData.NumericType.DOUBLE,
        null
    );

    private final String metricStatName;
    private final MetricStat metricStat;
    private final String field;
    private final ValueAggregator valueAggregators;
    private final StarTreeNumericType starTreeNumericType;
    private final DocIdSetIterator metricStatReader;

    /**
     * Constructor for MetricAggregationDescriptor
     */
    public MetricAggregationDescriptor(
        MetricStat metricStat,
        String field,
        IndexNumericFieldData.NumericType numericType,
        DocIdSetIterator metricStatReader
    ) {
        this.metricStat = metricStat;
        this.valueAggregators = ValueAggregatorFactory.getValueAggregator(metricStat);
        this.starTreeNumericType = StarTreeNumericType.fromNumericType(numericType);
        this.metricStatReader = metricStatReader;
        if (metricStat == MetricStat.COUNT) {
            this.field = STAR;
        } else {
            this.field = field;
        }
        this.metricStatName = toFieldName();
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
    public String getMetricStatName() {
        return metricStatName;
    }

    /**
     * @return aggregator for the field value
     */
    public ValueAggregator getValueAggregators() {
        return valueAggregators;
    }

    /**
     * @return star tree numeric type
     */
    public StarTreeNumericType getStarTreeNumericType() {
        return starTreeNumericType;
    }

    /**
     * @return metric value reader iterator
     */
    public DocIdSetIterator getMetricStatReader() {
        return metricStatReader;
    }

    /**
     * @return field name with metric type and field
     */
    public String toFieldName() {
        return toFieldName(metricStat, field);
    }

    /**
     * Builds field name with metric type and field
     */
    public static String toFieldName(MetricStat metricType, String field) {
        return metricType.getTypeName() + DELIMITER + field;
    }

    @Override
    public int hashCode() {
        return 31 * metricStat.hashCode() + field.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof MetricAggregationDescriptor) {
            MetricAggregationDescriptor anotherPair = (MetricAggregationDescriptor) obj;
            return metricStat == anotherPair.metricStat && field.equals(anotherPair.field);
        }
        return false;
    }

    @Override
    public String toString() {
        return toFieldName();
    }

    @Override
    public int compareTo(MetricAggregationDescriptor other) {
        return Comparator.comparing((MetricAggregationDescriptor o) -> o.field)
            .thenComparing((MetricAggregationDescriptor o) -> o.metricStat)
            .compare(this, other);
    }
}
