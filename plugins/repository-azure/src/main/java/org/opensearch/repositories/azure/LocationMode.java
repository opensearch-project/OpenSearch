/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.azure;

/**
 * Specifies the location mode used to decide which location the request should be sent to. The enumeration
 * simulates the semantics of the LocationMode, which is gone in the new SDKv12.
 */
public enum LocationMode {
    /**
     * Requests should always be sent to the primary location.
     */
    PRIMARY_ONLY,

    /**
     * Requests should always be sent to the primary location first. If the request fails, it should be sent to the
     * secondary location.
     */
    PRIMARY_THEN_SECONDARY,

    /**
     * Requests should always be sent to the secondary location.
     */
    SECONDARY_ONLY,

    /**
     * Requests should always be sent to the secondary location first. If the request fails, it should be sent to the
     * primary location.
     */
    SECONDARY_THEN_PRIMARY;
}
