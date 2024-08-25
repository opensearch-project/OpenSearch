/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

/**
 * This is stub at this point in time
 */
public class QueryGroupService {
    /**
     * updates the failure stats for the query group
     * @param queryGroupId query group identifier
     */
    public void requestFailedFor(final String queryGroupId) {

    }

    /**
     *
     * @param queryGroupId query group identifier
     * @return whether the queryGroup is contended and should reject new incoming requests
     */
    public boolean shouldRejectFor(String queryGroupId) {
        if (queryGroupId == null) return false;
        return false;
    }
}
