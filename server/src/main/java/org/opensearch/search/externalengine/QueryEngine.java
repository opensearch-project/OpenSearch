/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.externalengine;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteable;
import org.opensearch.core.xcontent.ToXContentObject;

/**
 * QueryEngine abstract interface.
 */
public abstract class QueryEngine implements NamedWriteable, ToXContentObject {
    public abstract void executeQuery(SearchRequest searchRequest,
                               ActionListener<SearchResponse> actionListener);
}
