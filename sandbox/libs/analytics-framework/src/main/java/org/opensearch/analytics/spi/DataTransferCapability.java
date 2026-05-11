/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

/**
 * Declares a backend's ability to produce or consume a shuffle-partitioned data transfer.
 *
 * <p>For hash-shuffle join the planner must intersect producer-side capabilities with consumer-side
 * capabilities to pick a serialization format the two sides agree on. A single backend typically
 * declares both {@link Kind#PRODUCER} and {@link Kind#CONSUMER} for the same format.
 *
 * <p>Formats in M2 are backend-specific strings — e.g. DataFusion declares
 * {@code "arrow-ipc-partitioned"} meaning "each partition's rows are shipped as Arrow IPC bytes";
 * a Velox-backed analytics backend could declare {@code "velox-native-serde"} (preserving Velox's
 * column-batch wire representation).
 *
 * @opensearch.internal
 */
public record DataTransferCapability(Kind kind, String format) {

    public enum Kind {
        /** Backend can partition a local stream and ship partitions to remote consumers. */
        PRODUCER,
        /** Backend can receive partitioned input streams and feed them into a worker plan. */
        CONSUMER
    }
}
