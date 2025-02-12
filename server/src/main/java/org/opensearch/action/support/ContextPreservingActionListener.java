/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.support;

import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;

import java.util.function.Supplier;

/**
 * Restores the given {@link org.opensearch.common.util.concurrent.ThreadContext.StoredContext}
 * once the listener is invoked
 *
 * @opensearch.internal
 */
public final class ContextPreservingActionListener<R> implements ActionListener<R> {

    private final ActionListener<R> delegate;
    private final Supplier<ThreadContext.StoredContext> context;

    public ContextPreservingActionListener(Supplier<ThreadContext.StoredContext> contextSupplier, ActionListener<R> delegate) {
        this.delegate = delegate;
        this.context = contextSupplier;
    }

    @Override
    public void onResponse(R r) {
        try (ThreadContext.StoredContext ignore = context.get()) {
            delegate.onResponse(r);
        }
    }

    @Override
    public void onFailure(Exception e) {
        try (ThreadContext.StoredContext ignore = context.get()) {
            delegate.onFailure(e);
        }
    }

    @Override
    public String toString() {
        return getClass().getName() + "/" + delegate.toString();
    }

    /**
     * Wraps the provided action listener in a {@link ContextPreservingActionListener} that will
     * also copy the response headers when the {@link ThreadContext.StoredContext} is closed
     */
    public static <R> ContextPreservingActionListener<R> wrapPreservingContext(ActionListener<R> listener, ThreadContext threadContext) {
        return new ContextPreservingActionListener<>(threadContext.newRestorableContext(true), listener);
    }
}
