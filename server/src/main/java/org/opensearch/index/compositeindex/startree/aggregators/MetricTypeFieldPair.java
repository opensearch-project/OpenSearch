/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.startree.aggregators;

import org.opensearch.index.compositeindex.MetricType;

import java.util.Comparator;

/**
 * Builds aggregation function and doc values field pair to support various aggregations
 * @opensearch.experimental
 */
public class MetricTypeFieldPair implements Comparable<MetricTypeFieldPair> {

    public static final String DELIMITER = "__";
    public static final String STAR = "*";
    public static final MetricTypeFieldPair COUNT_STAR = new MetricTypeFieldPair(MetricType.COUNT, STAR);

    private final MetricType metricType;
    private final String field;

    /**
     * Constructor for MetricTypeFieldPair
     */
    public MetricTypeFieldPair(MetricType metricType, String field) {
        this.metricType = metricType;
        if (metricType == MetricType.COUNT) {
            this.field = STAR;
        } else {
            this.field = field;
        }
    }

    /**
     * @return Metric Type
     */
    public MetricType getMetricType() {
        return metricType;
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
        return toFieldName(metricType, field);
    }

    /**
     * Builds field name with metric type and field
     */
    public static String toFieldName(MetricType metricType, String field) {
        return metricType.getTypeName() + DELIMITER + field;
    }

    /**
     * Builds MetricTypeFieldPair from field name
     */
    public static MetricTypeFieldPair fromFieldName(String fieldName) {
        String[] parts = fieldName.split(DELIMITER, 2);
        return fromMetricAndFieldName(parts[0], parts[1]);
    }

    /**
     * Builds MetricTypeFieldPair from metric and field name
     */
    private static MetricTypeFieldPair fromMetricAndFieldName(String metricName, String fieldName) {
        MetricType metricType = MetricType.fromTypeName(metricName);
        if (metricType == MetricType.COUNT) {
            return COUNT_STAR;
        } else {
            return new MetricTypeFieldPair(metricType, fieldName);
        }
    }

    @Override
    public int hashCode() {
        return 31 * metricType.hashCode() + field.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof MetricTypeFieldPair) {
            MetricTypeFieldPair anotherPair = (MetricTypeFieldPair) obj;
            return metricType == anotherPair.metricType && field.equals(anotherPair.field);
        }
        return false;
    }

    @Override
    public String toString() {
        return toFieldName();
    }

    @Override
    public int compareTo(MetricTypeFieldPair other) {
        return Comparator.comparing((MetricTypeFieldPair o) -> o.field)
            .thenComparing((MetricTypeFieldPair o) -> o.metricType)
            .compare(this, other);
    }
}
