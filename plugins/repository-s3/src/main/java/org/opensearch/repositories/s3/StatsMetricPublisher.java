/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.metrics.MetricRecord;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.blobstore.BlobStore;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class StatsMetricPublisher {

    private static final Logger LOGGER = LogManager.getLogger(StatsMetricPublisher.class);
    private final Stats stats = new Stats();

    private final Map<BlobStore.Metric, Stats> extendedStats = new HashMap<>() {
        {
            put(BlobStore.Metric.REQUEST_LATENCY, new Stats());
            put(BlobStore.Metric.REQUEST_SUCCESS, new Stats());
            put(BlobStore.Metric.REQUEST_FAILURE, new Stats());
            put(BlobStore.Metric.RETRY_COUNT, new Stats());
        }
    };

    public MetricPublisher listObjectsMetricPublisher = new MetricPublisher() {
        @Override
        public void publish(MetricCollection metricCollection) {
            LOGGER.debug(() -> "List objects request metrics: " + metricCollection);
            for (MetricRecord<?> metricRecord : metricCollection) {
                switch (metricRecord.metric().name()) {
                    case "ApiCallDuration":
                        extendedStats.get(BlobStore.Metric.REQUEST_LATENCY).listMetrics.addAndGet(
                            ((Duration) metricRecord.value()).toMillis()
                        );
                        break;
                    case "RetryCount":
                        extendedStats.get(BlobStore.Metric.RETRY_COUNT).listMetrics.addAndGet(((Integer) metricRecord.value()));
                        break;
                    case "ApiCallSuccessful":
                        if ((Boolean) metricRecord.value()) {
                            extendedStats.get(BlobStore.Metric.REQUEST_SUCCESS).listMetrics.addAndGet(1);
                        } else {
                            extendedStats.get(BlobStore.Metric.REQUEST_FAILURE).listMetrics.addAndGet(1);
                        }
                        stats.listMetrics.addAndGet(1);
                        break;
                }
            }
        }

        @Override
        public void close() {}
    };

    public MetricPublisher deleteObjectsMetricPublisher = new MetricPublisher() {
        @Override
        public void publish(MetricCollection metricCollection) {
            LOGGER.debug(() -> "Delete objects request metrics: " + metricCollection);
            for (MetricRecord<?> metricRecord : metricCollection) {
                switch (metricRecord.metric().name()) {
                    case "ApiCallDuration":
                        extendedStats.get(BlobStore.Metric.REQUEST_LATENCY).deleteMetrics.addAndGet(
                            ((Duration) metricRecord.value()).toMillis()
                        );
                        break;
                    case "RetryCount":
                        extendedStats.get(BlobStore.Metric.RETRY_COUNT).deleteMetrics.addAndGet(((Integer) metricRecord.value()));
                        break;
                    case "ApiCallSuccessful":
                        if ((Boolean) metricRecord.value()) {
                            extendedStats.get(BlobStore.Metric.REQUEST_SUCCESS).deleteMetrics.addAndGet(1);
                        } else {
                            extendedStats.get(BlobStore.Metric.REQUEST_FAILURE).deleteMetrics.addAndGet(1);
                        }
                        stats.deleteMetrics.addAndGet(1);
                        break;
                }
            }
        }

        @Override
        public void close() {}
    };

    public MetricPublisher getObjectMetricPublisher = new MetricPublisher() {
        @Override
        public void publish(MetricCollection metricCollection) {
            LOGGER.debug(() -> "Get object request metrics: " + metricCollection);
            for (MetricRecord<?> metricRecord : metricCollection) {
                switch (metricRecord.metric().name()) {
                    case "ApiCallDuration":
                        extendedStats.get(BlobStore.Metric.REQUEST_LATENCY).getMetrics.addAndGet(
                            ((Duration) metricRecord.value()).toMillis()
                        );
                        break;
                    case "RetryCount":
                        extendedStats.get(BlobStore.Metric.RETRY_COUNT).getMetrics.addAndGet(((Integer) metricRecord.value()));
                        break;
                    case "ApiCallSuccessful":
                        if ((Boolean) metricRecord.value()) {
                            extendedStats.get(BlobStore.Metric.REQUEST_SUCCESS).getMetrics.addAndGet(1);
                        } else {
                            extendedStats.get(BlobStore.Metric.REQUEST_FAILURE).getMetrics.addAndGet(1);
                        }
                        stats.getMetrics.addAndGet(1);
                        break;
                }
            }
        }

        @Override
        public void close() {}
    };

    public MetricPublisher putObjectMetricPublisher = new MetricPublisher() {
        @Override
        public void publish(MetricCollection metricCollection) {
            LOGGER.debug(() -> "Put object request metrics: " + metricCollection);
            for (MetricRecord<?> metricRecord : metricCollection) {
                switch (metricRecord.metric().name()) {
                    case "ApiCallDuration":
                        extendedStats.get(BlobStore.Metric.REQUEST_LATENCY).putMetrics.addAndGet(
                            ((Duration) metricRecord.value()).toMillis()
                        );
                        break;
                    case "RetryCount":
                        extendedStats.get(BlobStore.Metric.RETRY_COUNT).putMetrics.addAndGet(((Integer) metricRecord.value()));
                        break;
                    case "ApiCallSuccessful":
                        if ((Boolean) metricRecord.value()) {
                            extendedStats.get(BlobStore.Metric.REQUEST_SUCCESS).putMetrics.addAndGet(1);
                        } else {
                            extendedStats.get(BlobStore.Metric.REQUEST_FAILURE).putMetrics.addAndGet(1);
                        }
                        stats.putMetrics.addAndGet(1);
                        break;
                }
            }
        }

        @Override
        public void close() {}
    };

    public MetricPublisher multipartUploadMetricCollector = new MetricPublisher() {
        @Override
        public void publish(MetricCollection metricCollection) {
            LOGGER.debug(() -> "Multi-part request metrics: " + metricCollection);
            for (MetricRecord<?> metricRecord : metricCollection) {
                switch (metricRecord.metric().name()) {
                    case "ApiCallDuration":
                        extendedStats.get(BlobStore.Metric.REQUEST_LATENCY).multiPartPutMetrics.addAndGet(
                            ((Duration) metricRecord.value()).toMillis()
                        );
                        break;
                    case "RetryCount":
                        extendedStats.get(BlobStore.Metric.RETRY_COUNT).multiPartPutMetrics.addAndGet(((Integer) metricRecord.value()));
                        break;
                    case "ApiCallSuccessful":
                        if ((Boolean) metricRecord.value()) {
                            extendedStats.get(BlobStore.Metric.REQUEST_SUCCESS).multiPartPutMetrics.addAndGet(1);
                        } else {
                            extendedStats.get(BlobStore.Metric.REQUEST_FAILURE).multiPartPutMetrics.addAndGet(1);
                        }
                        stats.multiPartPutMetrics.addAndGet(1);
                        break;
                }
            }
        }

        @Override
        public void close() {}
    };

    public Stats getStats() {
        return stats;
    }

    public Map<BlobStore.Metric, Stats> getExtendedStats() {
        return extendedStats;
    }

    static class Stats {

        final AtomicLong listMetrics = new AtomicLong();

        final AtomicLong getMetrics = new AtomicLong();

        final AtomicLong putMetrics = new AtomicLong();

        final AtomicLong deleteMetrics = new AtomicLong();

        final AtomicLong multiPartPutMetrics = new AtomicLong();

        Map<String, Long> toMap() {
            final Map<String, Long> results = new HashMap<>();
            results.put("GetObject", getMetrics.get());
            results.put("ListObjects", listMetrics.get());
            results.put("PutObject", putMetrics.get());
            results.put("DeleteObjects", deleteMetrics.get());
            results.put("PutMultipartObject", multiPartPutMetrics.get());
            return results;
        }
    }
}
