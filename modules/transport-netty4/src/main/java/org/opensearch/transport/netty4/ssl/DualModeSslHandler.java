/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */
package org.opensearch.transport.netty4.ssl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.SecureTransportSettingsProvider;
import org.opensearch.transport.TcpTransport;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;

import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.ssl.SslHandler;

/**
 * Modifies the current pipeline dynamically to enable TLS
 *
 * @see <a href="https://github.com/opensearch-project/security/blob/d526c9f6c2a438c14db8b413148204510b9fe2e2/src/main/java/org/opensearch/security/ssl/transport/DualModeSSLHandler.java">DualModeSSLHandler</a>
 */
public class DualModeSslHandler extends ByteToMessageDecoder {

    private static final Logger logger = LogManager.getLogger(DualModeSslHandler.class);
    private final Settings settings;
    private final SecureTransportSettingsProvider secureTransportSettingsProvider;
    private final TcpTransport transport;
    private final SslHandler providedSSLHandler;

    public DualModeSslHandler(
        final Settings settings,
        final SecureTransportSettingsProvider secureTransportSettingsProvider,
        final TcpTransport transport
    ) {
        this(settings, secureTransportSettingsProvider, transport, null);
    }

    protected DualModeSslHandler(
        final Settings settings,
        final SecureTransportSettingsProvider secureTransportSettingsProvider,
        final TcpTransport transport,
        SslHandler providedSSLHandler
    ) {
        this.settings = settings;
        this.secureTransportSettingsProvider = secureTransportSettingsProvider;
        this.transport = transport;
        this.providedSSLHandler = providedSSLHandler;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // Will use the first six bytes to detect a protocol.
        if (in.readableBytes() < 6) {
            return;
        }
        int offset = in.readerIndex();
        if (in.getCharSequence(offset, 6, StandardCharsets.UTF_8).equals(SecureConnectionTestUtil.DUAL_MODE_CLIENT_HELLO_MSG)) {
            logger.debug("Received DualSSL Client Hello message");
            ByteBuf responseBuffer = Unpooled.buffer(6);
            responseBuffer.writeCharSequence(SecureConnectionTestUtil.DUAL_MODE_SERVER_HELLO_MSG, StandardCharsets.UTF_8);
            ctx.writeAndFlush(responseBuffer).addListener(ChannelFutureListener.CLOSE);
            return;
        }

        if (SslUtils.isTLS(in)) {
            logger.debug("Identified request as SSL request");
            enableSsl(ctx);
        } else {
            logger.debug("Identified request as non SSL request, running in HTTP mode as dual mode is enabled");
            ctx.pipeline().remove(this);
        }
    }

    private void enableSsl(ChannelHandlerContext ctx) throws SSLException, NoSuchAlgorithmException {
        final SSLEngine sslEngine = secureTransportSettingsProvider.buildSecureServerTransportEngine(settings, transport)
            .orElseGet(SslUtils::createDefaultServerSSLEngine);

        SslHandler sslHandler;
        if (providedSSLHandler != null) {
            sslHandler = providedSSLHandler;
        } else {
            sslHandler = new SslHandler(sslEngine);
        }
        ChannelPipeline p = ctx.pipeline();
        p.addAfter("port_unification_handler", "ssl_server", sslHandler);
        p.remove(this);
        logger.debug("Removed port unification handler and added SSL handler as incoming request is SSL");
    }
}
