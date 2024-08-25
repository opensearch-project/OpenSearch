/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.core.action.ActionListener;

public class QueryGroupFailureListener {
    public static <Response> ActionListener<Response> wrap(QueryGroupTask task, ActionListener<Response> originalListener) {
        return ActionListener.wrap(originalListener::onResponse, exception -> {
            // Call QueryGroup service to increment failures for query group of task
            System.out.println("QueryGroup listener is invoked");
            originalListener.onFailure(exception);
        });
    }
}
