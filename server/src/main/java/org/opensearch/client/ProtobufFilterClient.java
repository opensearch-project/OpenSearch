/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.client;

import org.opensearch.action.ActionType;
import org.opensearch.action.ProtobufActionRequest;
import org.opensearch.action.ProtobufActionType;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ProtobufActionResponse;
import org.opensearch.client.support.AbstractClient;
import org.opensearch.client.support.ProtobufAbstractClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;

/**
 * A {@link Client} that contains another {@link Client} which it
 * uses as its basic source, possibly transforming the requests / responses along the
 * way or providing additional functionality.
 *
 * @opensearch.internal
 */
public abstract class ProtobufFilterClient extends ProtobufAbstractClient {

    protected final ProtobufClient in;

    /**
     * Creates a new FilterClient
     *
     * @param in the client to delegate to
     * @see #in()
     */
    public ProtobufFilterClient(ProtobufClient in) {
        this(in.settings(), in.threadPool(), in);
    }

    /**
     * A Constructor that allows to pass settings and threadpool separately. This is useful if the
     * client is a proxy and not yet fully constructed ie. both dependencies are not available yet.
     */
    protected ProtobufFilterClient(Settings settings, ThreadPool threadPool, ProtobufClient in) {
        super(settings, threadPool);
        this.in = in;
    }

    @Override
    public void close() {
        in().close();
    }

    @Override
    protected <Request extends ProtobufActionRequest, Response extends ProtobufActionResponse> void doExecute(
        ProtobufActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        in().execute(action, request, listener);
    }

    /**
     * Returns the delegate {@link ProtobufClient}
     */
    protected ProtobufClient in() {
        return in;
    }
}
