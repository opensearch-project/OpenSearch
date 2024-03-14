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
import org.opensearch.transport.reactor.netty4.Netty4Utils;

import java.net.InetSocketAddress;

import io.netty.channel.Channel;

class ReactorNetty4HttpServerChannel implements HttpServerChannel {
    private final Channel channel;
    private final CompletableContext<Void> closeContext = new CompletableContext<>();

    ReactorNetty4HttpServerChannel(Channel channel) {
        this.channel = channel;
        Netty4Utils.addListener(this.channel.closeFuture(), closeContext);
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) channel.localAddress();
    }

    @Override
    public void addCloseListener(ActionListener<Void> listener) {
        closeContext.addListener(ActionListener.toBiConsumer(listener));
    }

    @Override
    public boolean isOpen() {
        return channel.isOpen();
    }

    @Override
    public void close() {
        channel.close();
    }

    @Override
    public String toString() {
        return "ReactorNetty4HttpChannel{localAddress=" + getLocalAddress() + "}";
    }
}
