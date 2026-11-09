/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4;

import org.opensearch.http.HttpPipelinedResponse;

import io.netty.util.ReferenceCounted;

/**
 * Shares the delegate's reference count so Netty can release responses rejected before the pipelining handler runs.
 * Wrapping does not retain the delegate: the handler transfers the same reference to Netty when it unwraps a ready response.
 */
final class Netty4HttpPipelinedResponse extends HttpPipelinedResponse implements ReferenceCounted {

    private final Netty4HttpResponse delegate;

    Netty4HttpPipelinedResponse(int sequence, Netty4HttpResponse delegate) {
        super(sequence, delegate);
        this.delegate = delegate;
    }

    @Override
    public int refCnt() {
        return delegate.refCnt();
    }

    @Override
    public Netty4HttpPipelinedResponse retain() {
        delegate.retain();
        return this;
    }

    @Override
    public Netty4HttpPipelinedResponse retain(int increment) {
        delegate.retain(increment);
        return this;
    }

    @Override
    public Netty4HttpPipelinedResponse touch() {
        delegate.touch();
        return this;
    }

    @Override
    public Netty4HttpPipelinedResponse touch(Object hint) {
        delegate.touch(hint);
        return this;
    }

    @Override
    public boolean release() {
        return delegate.release();
    }

    @Override
    public boolean release(int decrement) {
        return delegate.release(decrement);
    }
}
