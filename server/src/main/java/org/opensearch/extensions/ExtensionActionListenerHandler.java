/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.extensions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;

/**
 * Handles ActionListener requests from extensions
 *
 * @opensearch.internal
 */
public class ExtensionActionListenerHandler {

    private static final Logger logger = LogManager.getLogger(ExtensionActionListener.class);
    private ExtensionActionListener<ExtensionBooleanResponse> listener;

    public ExtensionActionListenerHandler(ExtensionActionListener<ExtensionBooleanResponse> listener) {
        this.listener = listener;
    }

    /**
     * Handles a {@link ExtensionActionListenerOnFailureRequest}.
     *
     * @param request  The request to handle.
     * @return A {@link ExtensionBooleanResponse} indicating success.
     * @throws Exception if the request is not handled properly.
     */
    public ExtensionBooleanResponse handleExtensionActionListenerOnFailureRequest(ExtensionActionListenerOnFailureRequest request)
        throws Exception {
        try {
            listener.onFailure(new OpenSearchException(request.getFailureException()));
            return new ExtensionBooleanResponse(true);
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw e;
        }
    }
}
