package org.opensearch.search.profile.fetch;/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

import java.util.Locale;

/**
 * Timing points for fetch phase profiling.
 */
public enum FetchTimingType {
    /** Time spent executing the fetch phase. */
    EXECUTE_FETCH_PHASE;

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
