/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.decommission;

/**
 * An enumeration of the states during decommissioning and recommissioning.
 */
public enum DecommissionStatus {
    /**
     * Decommission process is initiated
     */
    DECOMMISSION_INIT("decommission_init"),
    /**
     * Decommission process has started, decommissioned nodes should be weighed away
     */
    DECOMMISSION_IN_PROGRESS("decommission_in_progress"),
    /**
     * Decommissioning awareness attribute completed
     */
    DECOMMISSION_SUCCESSFUL("decommission_successful"),
    /**
     * Decommission request failed
     */
    DECOMMISSION_FAILED("decommission_failed"),
    /**
     * Recommission request received, recommissioning process has started
     */
    RECOMMISSION_IN_PROGRESS("recommission_in_progress"),
    /**
     * Recommission request failed. No nodes should fail to join the cluster with decommission exception
     */
    RECOMMISSION_FAILED("recommission_failed");

    private final String status;

    DecommissionStatus(String status) {
        this.status = status;
    }

    /**
     * Returns status that represents the decommission state
     *
     * @return status
     */
    public String status() {
        return status;
    }

    /**
     * Generate decommission status from given string
     *
     * @param status status in string
     * @return status
     */
    public static DecommissionStatus fromString(String status) {
        if (status == null) {
            throw new IllegalArgumentException("decommission status cannot be null");
        }
        if (status.equals(DECOMMISSION_INIT.status())) {
            return DECOMMISSION_INIT;
        } else if (status.equals(DECOMMISSION_IN_PROGRESS.status())) {
            return DECOMMISSION_IN_PROGRESS;
        } else if (status.equals(DECOMMISSION_SUCCESSFUL.status())) {
            return DECOMMISSION_SUCCESSFUL;
        } else if (status.equals(DECOMMISSION_FAILED.status())) {
            return DECOMMISSION_FAILED;
        } else if (status.equals(RECOMMISSION_IN_PROGRESS.status())) {
            return RECOMMISSION_IN_PROGRESS;
        } else if (status.equals(RECOMMISSION_FAILED.status())) {
            return RECOMMISSION_FAILED;
        }
        throw new IllegalStateException("Decommission status [" + status + "] not recognized.");
    }
}
