/*
 * Copyright OpenSearch Contributors.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.index.reindex.spi;

import org.opensearch.core.action.ActionListener;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.ReindexModulePlugin;
import org.opensearch.index.reindex.ReindexRequest;

/**
 * This interface provides an extension point for {@link ReindexModulePlugin}.
 * This interface can be implemented to provide a custom Rest interceptor and {@link ActionListener}
 * The Rest interceptor can be used to pre-process any reindex request and perform any action
 * on the response. The ActionListener listens to the success and failure events on every reindex request
 * and can be used to take any actions based on the success or failure.
 */
public interface RemoteReindexExtension {
    /**
     * Get an InterceptorProvider.
     * @return ReindexRestInterceptorProvider implementation.
     */
    ReindexRestInterceptorProvider getInterceptorProvider();

    /**
     * Get a wrapper of ActionListener which is can used to perform any action based on
     * the success/failure of the remote reindex call.
     * @return ActionListener wrapper implementation.
     */
    ActionListener<BulkByScrollResponse> getRemoteReindexActionListener(
        ActionListener<BulkByScrollResponse> listener,
        ReindexRequest reindexRequest
    );
}
