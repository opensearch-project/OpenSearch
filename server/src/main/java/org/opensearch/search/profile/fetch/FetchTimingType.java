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
    SORT_DOC_IDS,
    CREATE_STORED_FIELDS_VISITOR,
    BUILD_SUB_PHASE_PROCESSORS,
    GET_LEAF_READER,
    PREPARE_HIT_CONTEXT,
    EXPLAIN,
    FETCH_DOC_VALUES,
    SCRIPT_FIELDS,
    FETCH_SOURCE,
    FETCH_FIELDS,
    FETCH_VERSION,
    SEQ_NO_PRIMARY_TERM,
    MATCHED_QUERIES,
    HIGHLIGHT,
    FETCH_SCORE,
    BUILD_SEARCH_HITS;

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
