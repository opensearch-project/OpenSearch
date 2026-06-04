/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.decommission;

import org.opensearch.common.annotation.PublicApi;

/**
 * An enumeration of the states during decommissioning
 *
 * @opensearch.api
 */
@PublicApi(since = "2.4.0")
public enum DecommissionStatus {
    /**
     * Decommission process is initiated, and to-be-decommissioned leader is excluded from voting config
     */
    INIT("init"),
    /**
     * Decommission process is initiated, and the zone is being drained.
     */
    DRAINING("draining"),

    /**
     * Decommission process has started, decommissioned nodes should be removed
     */
    IN_PROGRESS("in_progress"),
    /**
     * Decommission action completed
     */
    SUCCESSFUL("successful"),
    /**
     * Decommission request failed
     */
    FAILED("failed");

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
        if (status.equals(INIT.status())) {
            return INIT;
        } else if (status.equals(DRAINING.status())) {
            return DRAINING;
        } else if (status.equals(IN_PROGRESS.status())) {
            return IN_PROGRESS;
        } else if (status.equals(SUCCESSFUL.status())) {
            return SUCCESSFUL;
        } else if (status.equals(FAILED.status())) {
            return FAILED;
        }
        throw new IllegalStateException("Decommission status [" + status + "] not recognized.");
    }
}
