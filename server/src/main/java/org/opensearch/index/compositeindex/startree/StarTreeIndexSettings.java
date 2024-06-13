/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.startree;

import org.opensearch.common.Rounding;
import org.opensearch.common.settings.Setting;
import org.opensearch.index.compositeindex.MetricType;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;

import java.util.Arrays;
import java.util.List;

/**
 * Index settings for star tree fields
 *
 * @opensearch.experimental
 */
public class StarTreeIndexSettings {
    /**
     * This setting determines the max number of star tree fields that can be part of composite index mapping. For each
     * star tree field, we will generate associated star tree index.
     */
    public static final Setting<Integer> STAR_TREE_MAX_FIELDS_SETTING = Setting.intSetting(
        "index.composite.star_tree.max_fields",
        1,
        1,
        1,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    /**
     * This setting determines the max number of dimensions that can be part of star tree index field. Number of
     * dimensions and associated cardinality has direct effect of star tree index size and query performance.
     */
    public static final Setting<Integer> STAR_TREE_MAX_DIMENSIONS_SETTING = Setting.intSetting(
        "index.composite.star_tree.field.max_dimensions",
        10,
        2,
        10,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    /**
     * This setting configures the default "maxLeafDocs" setting of star tree. This affects both query performance and
     * star tree index size. Lesser the leaves, better the query latency but higher storage size and vice versa
     * <p>
     * We can remove this later or change it to an enum based constant setting.
     *
     * @opensearch.experimental
     */
    public static final Setting<Integer> STAR_TREE_DEFAULT_MAX_LEAF_DOCS = Setting.intSetting(
        "index.composite.star_tree.default.max_leaf_docs",
        10000,
        1,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    /**
     * Default intervals for date dimension as part of star tree fields
     */
    public static final Setting<List<Rounding.DateTimeUnit>> DEFAULT_DATE_INTERVALS = Setting.listSetting(
        "index.composite.star_tree.field.default.date_intervals",
        Arrays.asList(Rounding.DateTimeUnit.MINUTES_OF_HOUR.shortName(), Rounding.DateTimeUnit.HOUR_OF_DAY.shortName()),
        StarTreeIndexSettings::getTimeUnit,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    /**
     * Default metrics for metrics as part of star tree fields
     */
    public static final Setting<List<MetricType>> DEFAULT_METRICS_LIST = Setting.listSetting(
        "index.composite.star_tree.field.default.metrics",
        Arrays.asList(
            MetricType.AVG.toString(),
            MetricType.COUNT.toString(),
            MetricType.SUM.toString(),
            MetricType.MAX.toString(),
            MetricType.MIN.toString()
        ),
        MetricType::fromTypeName,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    public static Rounding.DateTimeUnit getTimeUnit(String expression) {
        if (!DateHistogramAggregationBuilder.DATE_FIELD_UNITS.containsKey(expression)) {
            throw new IllegalArgumentException("unknown calendar interval specified in star tree index config");
        }
        return DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(expression);
    }
}
