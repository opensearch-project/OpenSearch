/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import software.amazon.awssdk.http.HttpMetric;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class StatsMetricPublisher {

    private final Stats stats = new Stats();

    public MetricPublisher listObjectsMetricPublisher = new MetricPublisher() {
        @Override
        public void publish(MetricCollection metricCollection) {
            stats.listCount.addAndGet(
                metricCollection.children()
                    .stream()
                    .filter(
                        metricRecords -> metricRecords.name().equals("ApiCallAttempt")
                            && !metricRecords.metricValues(HttpMetric.HTTP_STATUS_CODE).isEmpty()
                    )
                    .count()
            );
        }

        @Override
        public void close() {}
    };

    public MetricPublisher getObjectMetricPublisher = new MetricPublisher() {
        @Override
        public void publish(MetricCollection metricCollection) {
            stats.getCount.addAndGet(
                metricCollection.children()
                    .stream()
                    .filter(
                        metricRecords -> metricRecords.name().equals("ApiCallAttempt")
                            && !metricRecords.metricValues(HttpMetric.HTTP_STATUS_CODE).isEmpty()
                    )
                    .count()
            );
        }

        @Override
        public void close() {}
    };

    public MetricPublisher putObjectMetricPublisher = new MetricPublisher() {
        @Override
        public void publish(MetricCollection metricCollection) {
            stats.putCount.addAndGet(
                metricCollection.children()
                    .stream()
                    .filter(
                        metricRecords -> metricRecords.name().equals("ApiCallAttempt")
                            && !metricRecords.metricValues(HttpMetric.HTTP_STATUS_CODE).isEmpty()
                    )
                    .count()
            );
        }

        @Override
        public void close() {}
    };

    public MetricPublisher multipartUploadMetricCollector = new MetricPublisher() {
        @Override
        public void publish(MetricCollection metricCollection) {
            stats.postCount.addAndGet(
                metricCollection.children()
                    .stream()
                    .filter(
                        metricRecords -> metricRecords.name().equals("ApiCallAttempt")
                            && !metricRecords.metricValues(HttpMetric.HTTP_STATUS_CODE).isEmpty()
                    )
                    .count()
            );
        }

        @Override
        public void close() {}
    };

    public Stats getStats() {
        return stats;
    }

    static class Stats {

        final AtomicLong listCount = new AtomicLong();

        final AtomicLong getCount = new AtomicLong();

        final AtomicLong putCount = new AtomicLong();

        final AtomicLong postCount = new AtomicLong();

        Map<String, Long> toMap() {
            final Map<String, Long> results = new HashMap<>();
            results.put("GetObject", getCount.get());
            results.put("ListObjects", listCount.get());
            results.put("PutObject", putCount.get());
            results.put("PutMultipartObject", postCount.get());
            return results;
        }
    }
}
