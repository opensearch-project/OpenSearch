/*
 * Copyright OpenSearch Contributors.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.index.reindex.spi;

import org.apache.hc.core5.http.HttpRequestInterceptor;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.index.reindex.ReindexRequest;

import java.util.Optional;

public interface ReindexRestInterceptorProvider {
    /**
     * @param request Reindex request.
     * @param threadContext Current thread context.
     * @return HttpRequestInterceptor object.
     */
    Optional<HttpRequestInterceptor> getRestInterceptor(ReindexRequest request, ThreadContext threadContext);
}
