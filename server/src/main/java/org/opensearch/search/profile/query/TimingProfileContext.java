/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.query;

import org.opensearch.search.profile.AbstractTimingProfileBreakdown;

/**
 * A timing profile context
 */
public interface TimingProfileContext extends ProfileContext {
    @Override
    public AbstractTimingProfileBreakdown<QueryTimingType> context(Object context);
}
