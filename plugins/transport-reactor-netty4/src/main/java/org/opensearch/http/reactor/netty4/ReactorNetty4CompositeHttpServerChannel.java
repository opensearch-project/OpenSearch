/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.reactor.netty4;

import org.opensearch.common.concurrent.CompletableContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.http.HttpServerChannel;
import org.opensearch.transport.netty4.Netty4Utils;

import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

import io.netty.channel.Channel;

class ReactorNetty4CompositeHttpServerChannel implements HttpServerChannel {
    private final Channel[] channels;
    private final CompletableContext<Void>[] closeContexts;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    ReactorNetty4CompositeHttpServerChannel(Channel... channels) {
        if (channels == null || channels.length == 0) {
            throw new IllegalArgumentException("At least one channel must be provided");
        }

        this.channels = channels;
        this.closeContexts = new CompletableContext[channels.length];
        for (int i = 0; i < channels.length; ++i) {
            closeContexts[i] = new CompletableContext<>();
            Netty4Utils.addListener(this.channels[i].closeFuture(), closeContexts[i]);
        }
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) channels[0].localAddress();
    }

    @Override
    public void addCloseListener(ActionListener<Void> listener) {
        @SuppressWarnings({ "unchecked", "rawtypes" })
        final CompletableFuture<Void>[] futures = new CompletableFuture[closeContexts.length];
        for (int i = 0; i < closeContexts.length; ++i) {
            final CompletableFuture<Void> future = new CompletableFuture<>();
            closeContexts[i].addListener((v, t) -> {
                if (t == null) {
                    future.complete(v);
                } else {
                    future.completeExceptionally(t);
                }
            });
            futures[i] = future;
        }

        // Wait for all contexts to be closed
        CompletableFuture.allOf(futures).whenComplete((v, t) -> {
            if (t == null) {
                listener.onResponse(v);
            } else if (t instanceof Exception ex) {
                listener.onFailure(ex);
            } else {
                listener.onFailure(new UndeclaredThrowableException(t));
            }
        });
    }

    @Override
    public boolean isOpen() {
        for (int i = 0; i < channels.length; ++i) {
            if (channels[i].isOpen() == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void close() {
        for (int i = 0; i < channels.length; ++i) {
            channels[i].close();
        }
    }

    @Override
    public String toString() {
        return "ReactorNetty4CompositeHttpServerChannel{localAddress=" + getLocalAddress() + "}";
    }
}
