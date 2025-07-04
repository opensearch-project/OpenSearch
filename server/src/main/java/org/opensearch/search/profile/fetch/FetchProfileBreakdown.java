/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.fetch;

import org.opensearch.search.profile.AbstractProfileBreakdown;

public class FetchProfileBreakdown extends AbstractProfileBreakdown<FetchTimingType> {
    public FetchProfileBreakdown() {
        super(FetchTimingType.class);
    }
}
