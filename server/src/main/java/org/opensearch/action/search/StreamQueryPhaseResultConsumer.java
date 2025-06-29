/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.query.QuerySearchResult;

import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * Streaming query phase result consumer
 */
public class StreamQueryPhaseResultConsumer extends QueryPhaseResultConsumer {

    public StreamQueryPhaseResultConsumer(
        SearchRequest request,
        Executor executor,
        CircuitBreaker circuitBreaker,
        SearchPhaseController controller,
        SearchProgressListener progressListener,
        NamedWriteableRegistry namedWriteableRegistry,
        int expectedResultSize,
        Consumer<Exception> onPartialMergeFailure
    ) {
        super(
            request,
            executor,
            circuitBreaker,
            controller,
            progressListener,
            namedWriteableRegistry,
            expectedResultSize,
            onPartialMergeFailure
        );
    }

    void consumeStreamResult(SearchPhaseResult result, Runnable next) {
        // For streaming, we skip the ArraySearchPhaseResults.consumeResult() call
        // since it doesn't support multiple results from the same shard.
        QuerySearchResult querySearchResult = result.queryResult();
        pendingMerges.consume(querySearchResult, next);
    }
}
