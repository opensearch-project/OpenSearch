/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;

/**
 * This is stub at this point in time and will be replace by an acutal one in couple of days
 */
public class QueryGroupService {
    /**
     *
     * @param queryGroupId query group identifier
     */
    public void rejectIfNeeded(String queryGroupId) {
        if (queryGroupId == null) return;
        boolean reject = false;
        final StringBuilder reason = new StringBuilder();
        // TODO: At this point this is dummy and we need to decide whether to cancel the request based on last
        // reported resource usage for the queryGroup. We also need to increment the rejection count here for the
        // query group
        if (reject) {
            throw new OpenSearchRejectedExecutionException("QueryGroup " + queryGroupId + " is already contended." + reason.toString());
        }
    }
}
