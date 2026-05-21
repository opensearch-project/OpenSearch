/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.action;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Authorization probe request that extends {@link SearchRequest} so the security plugin's
 * legacy {@code IndexResolverReplacer} correctly resolves the target indices. The action
 * name ({@code indices:data/read/analytics/authorize}) matches the {@code indices:data/read*}
 * pattern, ensuring users with the standard {@code read} action group are authorized.
 */
public class AnalyticsAuthRequest extends SearchRequest {

    public AnalyticsAuthRequest(String... indices) {
        super(indices);
    }

    public AnalyticsAuthRequest(StreamInput in) throws IOException {
        super(in);
    }
}
