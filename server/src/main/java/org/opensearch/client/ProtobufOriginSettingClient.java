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
import org.opensearch.action.ProtobufActionResponse;
import org.opensearch.action.ProtobufActionType;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.support.ContextPreservingActionListener;
import org.opensearch.common.util.concurrent.ThreadContext;

import java.util.function.Supplier;

/**
 * A {@linkplain Client} that sends requests with the
 * {@link ThreadContext#stashWithOrigin origin} set to a particular
 * value and calls its {@linkplain ActionListener} in its original
 * {@link ThreadContext}.
 *
 * @opensearch.internal
 */
public final class ProtobufOriginSettingClient extends ProtobufFilterClient {

    private final String origin;

    public ProtobufOriginSettingClient(ProtobufClient in, String origin) {
        super(in);
        this.origin = origin;
    }

    @Override
    protected <Request extends ProtobufActionRequest, Response extends ProtobufActionResponse> void doExecute(
        ProtobufActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        final Supplier<ThreadContext.StoredContext> supplier = in().threadPool().getThreadContext().newRestorableContext(false);
        try (ThreadContext.StoredContext ignore = in().threadPool().getThreadContext().stashWithOrigin(origin)) {
            super.doExecute(action, request, new ContextPreservingActionListener<>(supplier, listener));
        }
    }
}
