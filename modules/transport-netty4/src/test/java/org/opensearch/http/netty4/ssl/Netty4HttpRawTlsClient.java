/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4.ssl;

import javax.net.ssl.SSLEngine;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.ssl.SslHandler;

/**
 * Drives TLS over a raw socket so tests can distinguish an authenticated close_notify from TCP EOF,
 * and send already-encrypted upload bytes after the server closes its output. An offline {@link SslHandler}
 * runs the TLS engine; this class only moves its records to and from the socket, one record at a time.
 */
final class Netty4HttpRawTlsClient implements Closeable {
    private final SslHandler ssl;
    private final EmbeddedChannel tls;
    private final Socket socket;

    /** The socket may be connected later, before {@link #handshake()}. */
    Netty4HttpRawTlsClient(SSLEngine engine, Socket socket) {
        ssl = new SslHandler(engine);
        tls = new EmbeddedChannel(ssl);
        this.socket = socket;
    }

    void handshake() throws IOException {
        while (ssl.handshakeFuture().isDone() == false) {
            writeEncrypted(encrypted());
            if (ssl.handshakeFuture().isDone() == false) {
                readRecord();
            }
        }
        writeEncrypted(encrypted());
        if (ssl.handshakeFuture().isSuccess() == false) {
            throw new IOException("TLS handshake failed", ssl.handshakeFuture().cause());
        }
    }

    byte[] encrypt(byte[] bytes) {
        tls.writeOutbound(Unpooled.wrappedBuffer(bytes));
        return encrypted();
    }

    void writeEncrypted(byte[] bytes) throws IOException {
        final OutputStream out = socket.getOutputStream();
        out.write(bytes);
        out.flush();
    }

    /** Returns the decrypted response. Fails if TCP ends before the server's close_notify. */
    byte[] readUntilCloseNotify() throws IOException {
        while (ssl.sslCloseFuture().isDone() == false) {
            readRecord();
        }
        if (ssl.sslCloseFuture().isSuccess() == false) {
            throw new IOException("TLS closed without close_notify", ssl.sslCloseFuture().cause());
        }
        final ByteArrayOutputStream plaintext = new ByteArrayOutputStream();
        for (ByteBuf buf; (buf = tls.readInbound()) != null; buf.release()) {
            plaintext.writeBytes(ByteBufUtil.getBytes(buf));
        }
        return plaintext.toByteArray();
    }

    /** Takes the records the engine has produced so far. */
    private byte[] encrypted() {
        final ByteArrayOutputStream encrypted = new ByteArrayOutputStream();
        for (ByteBuf buf; (buf = tls.readOutbound()) != null; buf.release()) {
            encrypted.writeBytes(ByteBufUtil.getBytes(buf));
        }
        return encrypted.toByteArray();
    }

    /** Reads exactly one record, so that nothing after close_notify is consumed. */
    private void readRecord() throws IOException {
        final InputStream in = socket.getInputStream();
        final byte[] header = in.readNBytes(5);
        if (header.length != 5) {
            throw new EOFException("TCP EOF before TLS close_notify");
        }
        final int length = (Byte.toUnsignedInt(header[3]) << 8) | Byte.toUnsignedInt(header[4]);
        final byte[] body = in.readNBytes(length);
        if (body.length != length) {
            throw new EOFException("TCP EOF inside a TLS record");
        }
        tls.writeInbound(Unpooled.wrappedBuffer(header, body));
    }

    @Override
    public void close() {
        tls.finishAndReleaseAll();
    }
}
