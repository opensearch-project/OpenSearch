/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

/**
 * Shuffle write capabilities that a backend may support.
 * Determines how data is partitioned and written during
 * hash/range exchange stages.
 *
 * @opensearch.internal
 */
public enum ShuffleCapability {
    /** Write Arrow IPC files locally, return shuffle manifest. */
    FILE_WRITE,
    /** Stream partitioned data directly to target nodes. Future enhancement. */
    STREAM_WRITE
}
