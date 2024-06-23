/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.aggregators;

import org.opensearch.index.compositeindex.datacube.MetricStat;

import java.util.Comparator;

/**
 * Builds aggregation function and doc values field pair to support various aggregations
 * @opensearch.experimental
 */
public class MetricStatFieldPair implements Comparable<MetricStatFieldPair> {

    public static final String DELIMITER = "__";
    public static final String STAR = "*";
    public static final MetricStatFieldPair COUNT_STAR = new MetricStatFieldPair(MetricStat.COUNT, STAR);

    private final MetricStat metricStat;
    private final String field;

    /**
     * Constructor for MetricStatFieldPair
     */
    public MetricStatFieldPair(MetricStat metricStat, String field) {
        this.metricStat = metricStat;
        if (metricStat == MetricStat.COUNT) {
            this.field = STAR;
        } else {
            this.field = field;
        }
    }

    /**
     * @return Metric Type
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

    /**
     * Builds MetricStatFieldPair from field name
     */
    public static MetricStatFieldPair fromFieldName(String fieldName) {
        String[] parts = fieldName.split(DELIMITER, 2);
        return fromMetricAndFieldName(parts[0], parts[1]);
    }

    /**
     * Builds MetricStatFieldPair from metric and field name
     */
    private static MetricStatFieldPair fromMetricAndFieldName(String metricName, String fieldName) {
        MetricStat metricType = MetricStat.fromTypeName(metricName);
        if (metricType == MetricStat.COUNT) {
            return COUNT_STAR;
        } else {
            return new MetricStatFieldPair(metricType, fieldName);
        }
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
        if (obj instanceof MetricStatFieldPair) {
            MetricStatFieldPair anotherPair = (MetricStatFieldPair) obj;
            return metricStat == anotherPair.metricStat && field.equals(anotherPair.field);
        }
        return false;
    }

    @Override
    public String toString() {
        return toFieldName();
    }

    @Override
    public int compareTo(MetricStatFieldPair other) {
        return Comparator.comparing((MetricStatFieldPair o) -> o.field)
            .thenComparing((MetricStatFieldPair o) -> o.metricStat)
            .compare(this, other);
    }
}
