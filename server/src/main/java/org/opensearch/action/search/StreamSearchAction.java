/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.ActionType;

/**
 * Transport action for executing a search
 *
 * @opensearch.internal
 */
public class StreamSearchAction extends ActionType<SearchResponse> {

    public static final StreamSearchAction INSTANCE = new StreamSearchAction();
    public static final String NAME = "indices:data/read/search/stream";

    private StreamSearchAction() {
        super(NAME, SearchResponse::new);
    }

}
