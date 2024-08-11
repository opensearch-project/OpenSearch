/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.composite.datacube.startree.fileformats.meta;

import org.opensearch.index.compositeindex.datacube.MetricStat;

import java.util.Objects;

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

    @Override
    public int hashCode() {
        return Objects.hashCode(metricFieldName + metricStat.getTypeName());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof MetricEntry) {
            MetricEntry anotherPair = (MetricEntry) obj;
            return metricStat.equals(anotherPair.metricStat) && metricFieldName.equals(anotherPair.metricFieldName);
        }
        return false;
    }

}
