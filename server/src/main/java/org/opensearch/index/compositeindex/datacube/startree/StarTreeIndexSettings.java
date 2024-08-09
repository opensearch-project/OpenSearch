/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree;

import org.opensearch.common.Rounding;
import org.opensearch.common.settings.Setting;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * Index settings for star tree fields. The settings are final as right now
 * there is no support for update of star tree mapping.
 *
 * @opensearch.experimental
 */
public class StarTreeIndexSettings {

    public static int STAR_TREE_MAX_DIMENSIONS_DEFAULT = 10;
    /**
     * This setting determines the max number of star tree fields that can be part of composite index mapping. For each
     * star tree field, we will generate associated star tree index.
     */
    public static final Setting<Integer> STAR_TREE_MAX_FIELDS_SETTING = Setting.intSetting(
        "index.composite_index.star_tree.max_fields",
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
        "index.composite_index.star_tree.field.max_dimensions",
        STAR_TREE_MAX_DIMENSIONS_DEFAULT,
        2,
        10,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    /**
     * This setting determines the max number of date intervals that can be part of star tree date field.
     */
    public static final Setting<Integer> STAR_TREE_MAX_DATE_INTERVALS_SETTING = Setting.intSetting(
        "index.composite_index.star_tree.field.max_date_intervals",
        3,
        1,
        3,
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
        "index.composite_index.star_tree.default.max_leaf_docs",
        10000,
        1,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    /**
     * Default intervals for date dimension as part of star tree fields
     */
    public static final Setting<List<Rounding.DateTimeUnit>> DEFAULT_DATE_INTERVALS = Setting.listSetting(
        "index.composite_index.star_tree.field.default.date_intervals",
        Arrays.asList(Rounding.DateTimeUnit.MINUTES_OF_HOUR.shortName(), Rounding.DateTimeUnit.HOUR_OF_DAY.shortName()),
        StarTreeIndexSettings::getTimeUnit,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    /**
     * Default metrics for metrics as part of star tree fields
     */
    public static final Setting<List<String>> DEFAULT_METRICS_LIST = Setting.listSetting(
        "index.composite_index.star_tree.field.default.metrics",
        Arrays.asList(MetricStat.COUNT.toString(), MetricStat.SUM.toString()),
        Function.identity(),
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    public static Rounding.DateTimeUnit getTimeUnit(String expression) {
        if (!DateHistogramAggregationBuilder.DATE_FIELD_UNITS.containsKey(expression)) {
            throw new IllegalArgumentException("unknown calendar intervals specified in star tree index mapping");
        }
        return DateHistogramAggregationBuilder.DATE_FIELD_UNITS.get(expression);
    }
}
