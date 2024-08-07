/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.composite.datacube.startree.fileformats.meta;

import org.opensearch.index.compositeindex.datacube.MetricStat;

/**
 * Holds the pair of metric name and it's associated stat
 *
 * @opensearch.experimental
 */
public class MetricEntry {

    private final String metricFieldName;
    private final MetricStat metricStat;

    public MetricEntry(String metricFieldName, MetricStat metricStat) {
        this.metricFieldName = metricFieldName;
        this.metricStat = metricStat;
    }

    public String getMetricFieldName() {
        return metricFieldName;
    }

    public MetricStat getMetricStat() {
        return metricStat;
    }
}
