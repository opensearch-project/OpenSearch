/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.threadpool;

import org.apache.lucene.util.SetOnce;

import java.util.function.Consumer;

public class RunnableTaskListenerFactory implements Consumer<RunnableTaskExecutionListener> {

    private final SetOnce<RunnableTaskExecutionListener> listener = new SetOnce<>();

    @Override
    public void accept(RunnableTaskExecutionListener runnableTaskExecutionListener) {
        listener.set(runnableTaskExecutionListener);
    }

    public RunnableTaskExecutionListener get() {
        assert listener.get() != null;
        return listener.get();
    }

}
