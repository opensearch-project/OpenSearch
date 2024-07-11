/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.tiering;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Index Tiering status
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public enum IndexTieringState {

    PENDING_START((byte) 0),

    /**
     * Tiering in progress (running shard relocation)
     */
    IN_PROGRESS((byte) 1),

    /**
     * Index tiered (shard relocation completed)
     */
    PENDING_COMPLETION((byte) 2),

    /**
     * Tiering finished successfully
     */
    COMPLETED((byte) 3),

    /**
     * Tiering failed
     */
    FAILED((byte) 4);

    private final byte value;

    /**
     * Constructs new state
     *
     * @param value state code
     */
    IndexTieringState(byte value) {
        this.value = value;
    }

    /**
     * Returns state code
     *
     * @return state code
     */
    public byte value() {
        return value;
    }

    /**
     * @return true if tiering is successful
     */
    public boolean successful() {
        return this == COMPLETED;
    }

    /**
     * @return true if tiering  is failed
     */
    public boolean failed() {
        return this == FAILED;
    }

    /**
     * Returns state corresponding to state code
     *
     * @param value stat code
     * @return state
     */
    public static IndexTieringState fromValue(byte value) {
        switch (value) {
            case 0:
                return PENDING_START;
            case 1:
                return IN_PROGRESS;
            case 2:
                return PENDING_COMPLETION;
            case 3:
                return COMPLETED;
            case 4:
                return FAILED;
            default:
                throw new IllegalArgumentException("No tiering state for value [" + value + "]");
        }
    }
}
