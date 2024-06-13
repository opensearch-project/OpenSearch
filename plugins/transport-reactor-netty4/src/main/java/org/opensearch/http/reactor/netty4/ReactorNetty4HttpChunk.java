/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.reactor.netty4;

import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.http.HttpChunk;
import org.opensearch.transport.reactor.netty4.Netty4Utils;

import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.buffer.ByteBuf;

class ReactorNetty4HttpChunk implements HttpChunk {
    private final AtomicBoolean released;
    private final boolean pooled;
    private final ByteBuf content;
    private final boolean last;

    ReactorNetty4HttpChunk(ByteBuf content, boolean last) {
        this.content = content;
        this.pooled = true;
        this.released = new AtomicBoolean(false);
        this.last = last;
    }

    @Override
    public BytesReference content() {
        assert released.get() == false;
        return Netty4Utils.toBytesReference(content);
    }

    @Override
    public void close() {
        if (pooled && released.compareAndSet(false, true)) {
            content.release();
        }
    }

    @Override
    public boolean isLast() {
        return last;
    }
}
