/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;

/**
 * Response handler for {@link UpdateSettingsRequest}
 *
 * @opensearch.internal
 */
public class UpdateSettingsResponseHandler implements TransportResponseHandler<ExtensionBooleanResponse> {
    private static final Logger logger = LogManager.getLogger(UpdateSettingsResponseHandler.class);

    @Override
    public ExtensionBooleanResponse read(StreamInput in) throws IOException {
        return new ExtensionBooleanResponse(in);
    }

    @Override
    public void handleResponse(ExtensionBooleanResponse response) {
        logger.info("response {}", response.getStatus());
    }

    @Override
    public void handleException(TransportException exp) {
        logger.error(new ParameterizedMessage("UpdateSettingsRequest failed"), exp);
    }

    @Override
    public String executor() {
        return ThreadPool.Names.GENERIC;
    }
}
