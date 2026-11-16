/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.action.ActionRequest;
import org.opensearch.common.lease.Releasable;
import org.opensearch.core.action.ActionListener;

/**
 * One check that {@link WorkloadGroupSearchAdmission} runs before a search or scroll page, such as a resource-limit
 * rejection or a throttle. Policies run in order; the first refusal fails the request.
 *
 * @opensearch.internal
 */
interface WorkloadGroupAdmissionPolicy {

    /** Whether this policy can currently refuse or charge any request. Read on every search, so it must be cheap. */
    boolean isActive();

    /** Whether this policy checks {@code request}; a policy that does not apply is skipped. */
    boolean appliesTo(WorkloadGroupTask task, ActionRequest request);

    /**
     * Admits {@code request}, completing {@code listener} with a non-null permit to close when the request completes
     * ({@link org.opensearch.action.search.SearchAdmission#NOOP_PERMIT} when nothing was charged), or with the exception the
     * request fails with. A throw before completing {@code listener} refuses the request. Once {@code listener} is completed,
     * a policy must not throw, except to propagate a throw from {@code listener}.
     */
    void admit(WorkloadGroupTask task, ActionRequest request, ActionListener<Releasable> listener);
}
