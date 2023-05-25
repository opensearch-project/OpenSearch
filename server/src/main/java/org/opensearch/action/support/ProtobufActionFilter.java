/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.support;

import org.opensearch.action.ActionListener;
import org.opensearch.action.ProtobufActionRequest;
import org.opensearch.action.ProtobufActionResponse;
import org.opensearch.tasks.ProtobufTask;

/**
 * A filter allowing to filter transport actions
*
* @opensearch.internal
*/
public interface ProtobufActionFilter {

    /**
     * The position of the filter in the chain. Execution is done from lowest order to highest.
    */
    int order();

    /**
     * Enables filtering the execution of an action on the request side, either by sending a response through the
    * {@link ActionListener} or by continuing the execution through the given {@link ActionFilterChain chain}
    */
    <Request extends ProtobufActionRequest, Response extends ProtobufActionResponse> void apply(
        ProtobufTask task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ProtobufActionFilterChain<Request, Response> chain
    );

    /**
     * A simple base class for injectable action filters that spares the implementation from handling the
    * filter chain. This base class should serve any action filter implementations that doesn't require
    * to apply async filtering logic.
    */
    abstract class Simple implements ProtobufActionFilter {
        @Override
        public final <Request extends ProtobufActionRequest, Response extends ProtobufActionResponse> void apply(
            ProtobufTask task,
            String action,
            Request request,
            ActionListener<Response> listener,
            ProtobufActionFilterChain<Request, Response> chain
        ) {
            if (apply(action, request, listener)) {
                chain.proceed(task, action, request, listener);
            }
        }

        /**
         * Applies this filter and returns {@code true} if the execution chain should proceed, or {@code false}
        * if it should be aborted since the filter already handled the request and called the given listener.
        */
        protected abstract boolean apply(String action, ProtobufActionRequest request, ActionListener<?> listener);
    }
}
