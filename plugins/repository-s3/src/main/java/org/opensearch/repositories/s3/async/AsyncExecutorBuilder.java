/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.async;

import java.util.concurrent.ExecutorService;

public class AsyncExecutorBuilder {

    private final ExecutorService futureCompletionExecutor;
    private final ExecutorService streamReader;
    private final TransferNIOGroup transferNIOGroup;

    public AsyncExecutorBuilder(ExecutorService futureCompletionExecutor,
                                ExecutorService streamReader,
                                TransferNIOGroup transferNIOGroup) {
        this.transferNIOGroup = transferNIOGroup;
        this.streamReader = streamReader;
        this.futureCompletionExecutor = futureCompletionExecutor;
    }

    public ExecutorService getFutureCompletionExecutor() {
        return futureCompletionExecutor;
    }

    public TransferNIOGroup getTransferNIOGroup() {
        return transferNIOGroup;
    }

    public ExecutorService getStreamReader() {
        return streamReader;
    }
}
