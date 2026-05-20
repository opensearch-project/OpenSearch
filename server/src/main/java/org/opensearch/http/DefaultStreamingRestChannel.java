/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http;

import org.opensearch.common.Nullable;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.ReleasableBytesStreamOutput;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.StreamingRestChannel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.reactivestreams.Subscriber;

import static org.opensearch.tasks.Task.X_OPAQUE_ID;

/**
 * The streaming rest channel for incoming requests. This class implements the logic for sending a streaming
 * rest in chunks response. It will set necessary headers and ensure that bytes are released after the full
 * response is sent.
 *
 * @opensearch.internal
 */
class DefaultStreamingRestChannel extends DefaultRestChannel implements StreamingRestChannel {
    private final StreamingHttpChannel streamingHttpChannel;
    @Nullable
    private final HttpTracer tracerLog;
    private final ThreadContext threadContext;

    DefaultStreamingRestChannel(
        StreamingHttpChannel streamingHttpChannel,
        HttpRequest httpRequest,
        RestRequest request,
        BigArrays bigArrays,
        HttpHandlingSettings settings,
        ThreadContext threadContext,
        CorsHandler corsHandler,
        @Nullable HttpTracer tracerLog
    ) {
        super(streamingHttpChannel, httpRequest, request, bigArrays, settings, threadContext, corsHandler, tracerLog);
        this.streamingHttpChannel = streamingHttpChannel;
        this.tracerLog = tracerLog;
        this.threadContext = threadContext;
    }

    @Override
    public void subscribe(Subscriber<? super HttpChunk> subscriber) {
        streamingHttpChannel.subscribe(subscriber);
    }

    @Override
    public void sendChunk(HttpChunk chunk) {
        String opaque = null;
        boolean success = false;
        final List<Releasable> toClose = new ArrayList<>(3);
        String contentLength = null;

        try {
            opaque = request.header(X_OPAQUE_ID);
            contentLength = String.valueOf(chunk.content().length());
            toClose.add(chunk);

            BytesStreamOutput bytesStreamOutput = newBytesOutput();
            if (bytesStreamOutput instanceof ReleasableBytesStreamOutput) {
                toClose.add((Releasable) bytesStreamOutput);
            }

            ActionListener<Void> listener = ActionListener.wrap(() -> Releasables.close(toClose));
            streamingHttpChannel.sendChunk(chunk, listener);
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(toClose);
            }
            if (tracerLog != null) {
                tracerLog.traceChunk(chunk, streamingHttpChannel, contentLength, opaque, request.getRequestId(), success);
            }
        }
    }

    @Override
    public void prepareResponse(RestStatus status, Map<String, List<String>> headers) {
        final Map<String, List<String>> enriched = new HashMap<>(headers);

        // Add all custom headers
        final Map<String, List<String>> customHeaders = threadContext.getResponseHeaders();
        if (customHeaders != null) {
            for (Map.Entry<String, List<String>> headerEntry : customHeaders.entrySet()) {
                for (String headerValue : headerEntry.getValue()) {
                    enriched.computeIfAbsent(headerEntry.getKey(), key -> new ArrayList<>()).add(headerValue);
                }
            }
        }

        final String opaque = request.header(X_OPAQUE_ID);
        if (opaque != null) {
            enriched.put(X_OPAQUE_ID, List.of(opaque));
        }

        streamingHttpChannel.prepareResponse(status.getStatus(), enriched);
    }

    @Override
    public void sendResponse(RestResponse restResponse) {
        prepareResponse(restResponse.status(), restResponse.getHeaders());
        super.sendResponse(restResponse);
    }

    @Override
    public boolean isReadable() {
        return streamingHttpChannel.isReadable();
    }

    @Override
    public boolean isWritable() {
        return streamingHttpChannel.isWritable();
    }
}
