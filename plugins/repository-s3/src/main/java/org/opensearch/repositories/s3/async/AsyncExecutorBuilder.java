/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.async;

import java.util.concurrent.ExecutorService;

/**
 * An encapsulation for the {@link TransferNIOGroup}, and the stream reader and future completion executor services
 */
public class AsyncExecutorBuilder {

    private final ExecutorService futureCompletionExecutor;
    private final ExecutorService streamReader;
    private final TransferNIOGroup transferNIOGroup;

    /**
     * Construct a new AsyncExecutorBuilder object
     *
     * @param futureCompletionExecutor An {@link ExecutorService} to pass to {@link software.amazon.awssdk.services.s3.S3AsyncClient} for future completion
     * @param streamReader An {@link ExecutorService} to read streams for upload
     * @param transferNIOGroup A {@link TransferNIOGroup} which encapsulates the netty {@link io.netty.channel.EventLoopGroup} for async uploads
     */
    public AsyncExecutorBuilder(ExecutorService futureCompletionExecutor, ExecutorService streamReader, TransferNIOGroup transferNIOGroup) {
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
