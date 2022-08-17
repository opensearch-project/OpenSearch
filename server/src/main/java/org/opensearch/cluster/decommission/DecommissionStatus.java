/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.decommission;

public enum DecommissionStatus {
    /**
     * Decommission process is initiated
     */
    INIT((byte) 0),
    /**
     * Decommission process has started, decommissioned nodes should be weighed away
     */
    DECOMMISSIONING((byte) 1),
    /**
     * Decommissioning awareness attribute completed
     */
    DECOMMISSIONED((byte) 2),
    /**
     * Decommission request failed
     */
    DECOMMISSION_FAILED((byte) 3),
    /**
     * Recommission request received, recommissioning process has started
     */
    RECOMMISSIONING((byte) 4),
    /**
     * Recommission request failed. No nodes should fail to join the cluster with decommission exception
     */
    RECOMMISSION_FAILED((byte) 5);

    private final byte value;

    DecommissionStatus(byte value) {
        this.value = value;
    }

    /**
     * Returns code that represents the decommission state
     *
     * @return code for the state
     */
    public byte value() {
        return value;
    }

    /**
     * Generate decommission state from code
     *
     * @param value the state code
     * @return state
     */
    public static DecommissionStatus fromValue(byte value) {
        switch (value) {
            case 0:
                return INIT;
            case 1:
                return DECOMMISSIONING;
            case 2:
                return DECOMMISSIONED;
            case 3:
                return DECOMMISSION_FAILED;
            case 4:
                return RECOMMISSIONING;
            case 5:
                return RECOMMISSION_FAILED;
            default:
                throw new IllegalArgumentException("No decommission state for value [" + value + "]");
        }
    }

    public static DecommissionStatus fromString(String status) {
        if ("init".equals(status)) {
            return INIT;
        } else if ("decommissioning".equals(status)) {
            return DECOMMISSIONING;
        } else if ("decommissioned".equals(status)) {
            return DECOMMISSIONED;
        } else if ("decommission_failed".equals(status)) {
            return DECOMMISSION_FAILED;
        } else if ("recommissioning".equals(status)) {
            return RECOMMISSIONING;
        } else if ("recommission_failed".equals(status)) {
            return RECOMMISSION_FAILED;
        }
        throw new IllegalStateException("No status match for [" + status + "]");
    }
}

