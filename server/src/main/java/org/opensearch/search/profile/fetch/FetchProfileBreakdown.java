/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.fetch;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.search.profile.AbstractProfileBreakdown;
import org.opensearch.search.profile.ProfileMetricUtil;

/**
 * A record of timings for the various operations that may happen during fetch execution.
 */
@ExperimentalApi()
public class FetchProfileBreakdown extends AbstractProfileBreakdown {
    public FetchProfileBreakdown() {
        super(ProfileMetricUtil.getFetchProfileMetrics());
    }
}
