/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.externalengine;

import java.io.IOException;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryBuilder;


/**
 * Query Engine Parser.
 * @param <T> extend QuerEgnien.
 */
@FunctionalInterface
public interface QueryEngineParser<T extends QueryEngine> {

    /**
     * Creates a new {@link QueryBuilder} from the query held by the
     * {@link XContentParser}. The state on the parser contained in this context
     * will be changed as a side effect of this method call
     */
    T fromXContent(XContentParser parser) throws IOException;

}
