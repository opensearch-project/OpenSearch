/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.fetch;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.search.profile.AbstractProfiler;

/**
 * Profiler for the fetch phase.
 */
@PublicApi(since = "3.2.0")
public class FetchProfiler extends AbstractProfiler<FetchProfileBreakdown, String> {
    /**
     * Creates a new FetchProfiler.
     */
    public FetchProfiler() {
        super(new InternalFetchProfileTree());
    }
}
