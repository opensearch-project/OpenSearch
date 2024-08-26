/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import java.util.Optional;

/**
 * This is stub at this point in time and will be replace by an acutal one in couple of days
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
    public Optional<String> shouldRejectFor(String queryGroupId) {
        if (queryGroupId == null) return Optional.empty();
        // TODO: At this point this is dummy and we need to decide whether to cancel the request based on last
        // reported resource usage for the queryGroup. We also need to increment the rejection count here for the
        // query group
        return Optional.of("Possible reason. ");
    }
}
