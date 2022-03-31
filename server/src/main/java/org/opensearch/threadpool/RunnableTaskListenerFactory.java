/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.threadpool;

import org.apache.lucene.util.SetOnce;

import java.util.function.Function;

public class RunnableTaskListenerFactory implements Function<RunnableTaskExecutionListener, RunnableTaskExecutionListener> {

    private final SetOnce<RunnableTaskExecutionListener> listener = new SetOnce<>();

    @Override
    public RunnableTaskExecutionListener apply(RunnableTaskExecutionListener runnableTaskExecutionListener) {
        listener.set(runnableTaskExecutionListener);
        return listener.get();
    }

    public RunnableTaskExecutionListener get() {
        assert listener.get() != null;
        return listener.get();
    }
}
