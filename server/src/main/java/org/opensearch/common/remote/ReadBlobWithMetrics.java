/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.remote;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * This class will be used to store response read from any blob store along with metrics
 * for time taken during serialization and reading of blob.
 *
 * @param <T>        Type of blob
 * @param blobEntity deserialized response of Type T, read from blob store
 * @param serDeMS    time spent in ms during deserializing the response
 * @param readMS     time spent in ms in reading the blob from blob store
 */
@ExperimentalApi
public record ReadBlobWithMetrics<T>(T blobEntity, long serDeMS, long readMS) implements Comparable<ReadBlobWithMetrics<?>> {

    /**
     * Compares two ReadBlobWithMetrics objects based on the total time taken for serialization and reading.
     * To be used for sorting the objects in ascending order of total time taken.
     */
    @Override
    public int compareTo(ReadBlobWithMetrics<?> other) {
        long thisTotalTime = this.readMS() + this.serDeMS();
        long otherTotalTime = other.readMS() + other.serDeMS();
        return Long.compare(thisTotalTime, otherTotalTime);
    }

}
