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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.transport.nio;

import org.opensearch.common.breaker.CircuitBreaker;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.bytes.CompositeBytesReference;
import org.opensearch.common.bytes.ReleasableBytesReference;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.core.internal.io.IOUtils;
import org.opensearch.nio.BytesWriteHandler;
import org.opensearch.nio.InboundChannelBuffer;
import org.opensearch.nio.Page;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.InboundPipeline;
import org.opensearch.transport.TcpTransport;
import org.opensearch.transport.Transport;

import java.io.IOException;
import java.util.function.Supplier;

public class TcpReadWriteHandler extends BytesWriteHandler {

    private final NioTcpChannel channel;
    private final InboundPipeline pipeline;

    public TcpReadWriteHandler(NioTcpChannel channel, PageCacheRecycler recycler, TcpTransport transport) {
        this.channel = channel;
        final ThreadPool threadPool = transport.getThreadPool();
        final Supplier<CircuitBreaker> breaker = transport.getInflightBreaker();
        final Transport.RequestHandlers requestHandlers = transport.getRequestHandlers();
        this.pipeline = new InboundPipeline(
            transport.getVersion(),
            transport.getStatsTracker(),
            recycler,
            threadPool::relativeTimeInMillis,
            breaker,
            requestHandlers::getHandler,
            transport::inboundMessage
        );
    }

    @Override
    public int consumeReads(InboundChannelBuffer channelBuffer) throws IOException {
        Page[] pages = channelBuffer.sliceAndRetainPagesTo(channelBuffer.getIndex());
        BytesReference[] references = new BytesReference[pages.length];
        for (int i = 0; i < pages.length; ++i) {
            references[i] = BytesReference.fromByteBuffer(pages[i].byteBuffer());
        }
        Releasable releasable = () -> IOUtils.closeWhileHandlingException(pages);
        try (ReleasableBytesReference reference = new ReleasableBytesReference(CompositeBytesReference.of(references), releasable)) {
            pipeline.handleBytes(channel, reference);
            return reference.length();
        }
    }

    @Override
    public void close() {
        Releasables.closeWhileHandlingException(pipeline);
        super.close();
    }
}
