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
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * JobDetailsResponseHandler to handle JobDetailsResponse from extensions
 *
 * @opensearch.internal
 */
public class JobDetailsResponseHandler implements TransportResponseHandler<JobDetailsResponse> {
    private static final Logger logger = LogManager.getLogger(JobDetailsResponseHandler.class);
    final CompletableFuture<JobDetailsResponse> inProgressFuture = new CompletableFuture<>();
    private Map<String, JobDetails> jobDetailsMap;

    private String uniqueExtensionId;

    public JobDetailsResponseHandler(Map<String, JobDetails> jobDetailsMap, String uniqueExtensionId) {
        this.jobDetailsMap = jobDetailsMap;
        this.uniqueExtensionId = uniqueExtensionId;
    }

    @Override
    public JobDetailsResponse read(StreamInput in) throws IOException {
        return new JobDetailsResponse(in);
    }

    @Override
    public void handleResponse(JobDetailsResponse response) {
        logger.info(response.getJobDetails().toString());
        jobDetailsMap.put(uniqueExtensionId, response.getJobDetails());
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
}
