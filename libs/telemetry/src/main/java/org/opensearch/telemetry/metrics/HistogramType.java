/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.metrics;

/**
 * Histogram type, primarily depends on bucketing strategy.
 */
public enum HistogramType {
    /**
     * Fixed buckets.
     */
    FIXED_BUCKET,
    /**
     * Buckets will be defined dynamically based on the value distribution.
     */
    DYNAMIC;
}
