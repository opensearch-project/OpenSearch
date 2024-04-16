/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.core.action.ActionListener;

import java.io.IOException;
import java.util.List;

/**
 * Hook for running code that needs to be executed before the upload of index metadata. Here we have introduced a hook
 * for index creation (also triggerred after enabling the remote cluster statement for the first time). The Interceptor
 * is intended to be run in parallel and async with the index metadata upload.
 *
 * @opensearch.internal
 */
public interface IndexMetadataUploadInterceptor {

    /**
     * Intercepts the index metadata upload flow with input containing index metadata of new indexes (or first time upload).
     * The caller is expected to trigger onSuccess or onFailure of the {@code ActionListener}.
     *
     * @param indexMetadataList list of index metadata of new indexes (or first time index metadata upload).
     * @param actionListener    listener to be invoked on success or failure.
     */
    void interceptIndexCreation(List<IndexMetadata> indexMetadataList, ActionListener<Void> actionListener) throws IOException;
}
