/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search.grpc;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.arrow.search.ArrowSearchPlugin;
import org.opensearch.arrow.search.action.ArrowSearchAction;
import org.opensearch.arrow.search.action.ArrowSearchRequest;
import org.opensearch.core.action.ActionListener;
import org.opensearch.transport.grpc.spi.StreamSearchFormatHandler;
import org.opensearch.transport.grpc.spi.StreamSearchResponseChannel;

/**
 * Serves {@code ServerStreamSearch} calls with {@code x-opensearch-format: arrow}.
 * <p>
 * The response stream carries, in {@code SearchResponse} payload messages, an Arrow IPC schema message followed by one
 * record batch message per batch. A regular {@code SearchResponse} summary ends it, and its total hits count is the number
 * of rows.
 */
public final class ArrowStreamSearchFormatHandler implements StreamSearchFormatHandler {

    /** The value of the {@code x-opensearch-format} header that this handler serves. */
    public static final String FORMAT = "arrow";

    private final ArrowSearchPlugin plugin;

    /**
     * Creates the handler of the {@value #FORMAT} format.
     *
     * @param plugin the plugin, which provides the node client, settings and executor once the node starts
     */
    public ArrowStreamSearchFormatHandler(ArrowSearchPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public String format() {
        return FORMAT;
    }

    @Override
    public void execute(SearchRequest request, StreamSearchResponseChannel channel) {
        final ArrowSearchPlugin.Components components = plugin.components();
        final GrpcArrowBatchSink sink = new GrpcArrowBatchSink(channel, components.settings()::clientIdleTimeout, components.executor());
        final ArrowSearchRequest arrowRequest;
        try {
            arrowRequest = new ArrowSearchRequest(request, sink, components.settings().cancelAfterTimeInterval());
        } catch (RuntimeException e) {
            sink.fail(e);
            return;
        }
        // Every outcome ends the stream through this listener, including failures of validation or security before the action runs.
        components.client().execute(ArrowSearchAction.INSTANCE, arrowRequest, ActionListener.wrap(sink::complete, sink::fail));
    }
}
