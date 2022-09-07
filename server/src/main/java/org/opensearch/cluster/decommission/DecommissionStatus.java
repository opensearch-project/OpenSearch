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
    INIT("init", 0),
    /**
     * Exclude cluster manager from Voting Configuration
     */
    EXCLUDE_LEADER_FROM_VOTING_CONFIG("exclude_leader_from_voting_config", 2),
    /**
     * Decommission process has started, decommissioned nodes should be removed
     */
    IN_PROGRESS("in_progress", 3),
    /**
     * Decommission action completed
     */
    SUCCESSFUL("successful", 4),
    /**
     * Decommission request failed
     */
    FAILED("failed", -1);

    private final String status;
    private final int stage;

    DecommissionStatus(String status, int stage) {
        this.status = status;
        this.stage = stage;
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
     * Returns stage that represents the decommission stage
     */
    public int stage() {
        return stage;
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
        if (status.equals(INIT.status())) {
            return INIT;
        } else if (status.equals(EXCLUDE_LEADER_FROM_VOTING_CONFIG.status())) {
            return EXCLUDE_LEADER_FROM_VOTING_CONFIG;
        } else if (status.equals(IN_PROGRESS.status())) {
            return IN_PROGRESS;
        } else if (status.equals(SUCCESSFUL.status())) {
            return SUCCESSFUL;
        } else if (status.equals(FAILED.status())) {
            return FAILED;
        }
        throw new IllegalStateException("Decommission status [" + status + "] not recognized.");
    }

    /**
     * Generate decommission status from given stage
     *
     * @param stage stage in int
     * @return status
     */
    public static DecommissionStatus fromStage(int stage) {
        if (stage == INIT.stage()) {
            return INIT;
        } else if (stage == EXCLUDE_LEADER_FROM_VOTING_CONFIG.stage()) {
            return EXCLUDE_LEADER_FROM_VOTING_CONFIG;
        } else if (stage == IN_PROGRESS.stage()) {
            return IN_PROGRESS;
        } else if (stage == SUCCESSFUL.stage()) {
            return SUCCESSFUL;
        } else if (stage == FAILED.stage()) {
            return FAILED;
        }
        throw new IllegalStateException("Decommission stage [" + stage + "] not recognized.");
    }
}
