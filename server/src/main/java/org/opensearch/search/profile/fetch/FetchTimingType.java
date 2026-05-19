/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.fetch;

import java.util.Locale;

/**
 * Timing points for fetch phase profiling.
 */
public enum FetchTimingType {
    /** Time spent creating the stored fields visitor */
    CREATE_STORED_FIELDS_VISITOR,
    /** Time spent building fetch sub-phase processors */
    BUILD_SUB_PHASE_PROCESSORS,
    /** Time spent getting the specific leaf reader context for a given segment index */
    GET_NEXT_READER,
    /** Time spent setting next reader for each subphase processor **/
    SET_NEXT_READER,
    /** Time spent loading stored fields for a hit */
    LOAD_STORED_FIELDS,
    /** Time spent loading the document _source */
    LOAD_SOURCE,
    /** Time spent executing a fetch sub-phase */
    PROCESS;

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
