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
import org.opensearch.common.io.stream.NamedWriteableRegistryParseResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;

/**
 * Response handler for {@link NamedWriteableRegistryParseResponse} 
 *
 * @opensearch.internal
 */
public class NamedWriteableRegistryParseResponseHandler implements TransportResponseHandler<NamedWriteableRegistryParseResponse> {
    private static final Logger logger = LogManager.getLogger(NamedWriteableRegistryParseResponseHandler.class);

    @Override
    public NamedWriteableRegistryParseResponse read(StreamInput in) throws IOException {
        return new NamedWriteableRegistryParseResponse(in);
    }

    @Override
    public void handleResponse(NamedWriteableRegistryParseResponse response) {
        logger.info("response {}", response.getStatus());
    }

    @Override
    public void handleException(TransportException exp) {
        logger.error(new ParameterizedMessage("NamedWriteableRegistryParseRequest failed", exp));
    }

    @Override
    public String executor() {
        return ThreadPool.Names.GENERIC;
    }
}
