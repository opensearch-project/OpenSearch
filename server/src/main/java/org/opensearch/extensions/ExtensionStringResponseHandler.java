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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * ExtensionStringResponseHandler to handle ExtensionStringResponse from extensions
 *
 * @opensearch.internal
 */
public class ExtensionStringResponseHandler implements TransportResponseHandler<ExtensionStringResponse> {
    private static final Logger logger = LogManager.getLogger(ExtensionStringResponseHandler.class);
    final CompletableFuture<ExtensionStringResponse> inProgressFuture = new CompletableFuture<>();
    private String response;

    public ExtensionStringResponseHandler(String response) {
        this.response = response;
    }

    @Override
    public ExtensionStringResponse read(StreamInput in) throws IOException {
        return new ExtensionStringResponse(in);
    }

    @Override
    public void handleResponse(ExtensionStringResponse response) {
        logger.info("Response from extension  " + response);
        this.response = response.getResponse();
        inProgressFuture.complete(response);
    }

    @Override
    public void handleException(TransportException exp) {
        logger.error(new ParameterizedMessage("Fetch Job Details from extension failed"), exp);
        inProgressFuture.completeExceptionally(exp);
    }

    @Override
    public String executor() {
        return ThreadPool.Names.GENERIC;
    }

    public String getResponse() {
        return response;
    }

    public void setResponse(String response) {
        this.response = response;
    }

    @Override
    public String toString() {
        return "ExtensionStringResponseHandler{" + "response='" + response + '\'' + '}';
    }
}
