/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.query;

import org.opensearch.search.profile.AbstractProfileBreakdown;
import org.opensearch.search.profile.AbstractTimingProfileBreakdown;
import org.opensearch.search.profile.Timer;

/**
 * A {@link AbstractTimingProfileBreakdown} for query timings.
 */
public class QueryTimingProfileBreakdown extends AbstractTimingProfileBreakdown<QueryTimingType> implements TimingProfileContext {

    public QueryTimingProfileBreakdown() {
        for(QueryTimingType type : QueryTimingType.values()) {
            timers.put(type, new Timer());
        }
    }

    @Override
    public AbstractTimingProfileBreakdown<QueryTimingType> context(Object context) {
        return this;
    }
}
