/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportService;

/**
 *  NodeConnectionsService for StreamTransportService
 */
@ExperimentalApi
public class StreamNodeConnectionsService extends NodeConnectionsService {
    @Inject
    public StreamNodeConnectionsService(Settings settings, ThreadPool threadPool, StreamTransportService streamTransportService) {
        super(settings, threadPool, streamTransportService);
    }
}
