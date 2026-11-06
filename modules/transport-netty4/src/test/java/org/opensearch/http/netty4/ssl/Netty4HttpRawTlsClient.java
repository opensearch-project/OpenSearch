/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4.ssl;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

/**
 * Drives TLS over a raw socket so tests can distinguish an authenticated close_notify from TCP EOF,
 * and send already-encrypted upload bytes after the server closes its output.
 */
final class Netty4HttpRawTlsClient {
    private final SSLEngine engine;
    private final InputStream in;
    private final OutputStream out;
    private final ByteArrayOutputStream plaintext = new ByteArrayOutputStream();
    private boolean closeNotify;

    Netty4HttpRawTlsClient(SSLEngine engine, Socket socket) throws IOException {
        this.engine = engine;
        in = socket.getInputStream();
        out = socket.getOutputStream();
    }

    void handshake() throws IOException {
        engine.beginHandshake();
        while (true) {
            switch (engine.getHandshakeStatus()) {
                case NEED_TASK -> runTasks();
                case NEED_WRAP -> writeEncrypted(wrap(ByteBuffer.allocate(0)));
                case NEED_UNWRAP -> readRecord();
                case NEED_UNWRAP_AGAIN -> unwrap(ByteBuffer.allocate(0));
                case FINISHED, NOT_HANDSHAKING -> {
                    return;
                }
            }
        }
    }

    byte[] encrypt(byte[] bytes) throws IOException {
        final ByteBuffer input = ByteBuffer.wrap(bytes);
        final ByteArrayOutputStream encrypted = new ByteArrayOutputStream();
        while (input.hasRemaining()) {
            encrypted.writeBytes(wrap(input));
        }
        return encrypted.toByteArray();
    }

    void writeEncrypted(byte[] bytes) throws IOException {
        out.write(bytes);
        out.flush();
    }

    byte[] readUntilCloseNotify() throws IOException {
        while (closeNotify == false) {
            readRecord();
        }
        return plaintext.toByteArray();
    }

    private byte[] wrap(ByteBuffer input) throws IOException {
        final ByteBuffer packet = ByteBuffer.allocate(engine.getSession().getPacketBufferSize());
        final SSLEngineResult result = engine.wrap(input, packet);
        if (result.getStatus() != SSLEngineResult.Status.OK || result.bytesConsumed() + result.bytesProduced() == 0) {
            throw new IOException("TLS wrap made no progress: " + result);
        }
        packet.flip();
        final byte[] bytes = new byte[packet.remaining()];
        packet.get(bytes);
        return bytes;
    }

    private void readRecord() throws IOException {
        final byte[] header = in.readNBytes(5);
        if (header.length != 5) {
            throw new EOFException("TCP EOF before TLS close_notify");
        }
        final int length = (Byte.toUnsignedInt(header[3]) << 8) | Byte.toUnsignedInt(header[4]);
        final byte[] body = in.readNBytes(length);
        if (body.length != length) {
            throw new EOFException("TCP EOF inside a TLS record");
        }
        final ByteBuffer record = ByteBuffer.allocate(header.length + length);
        record.put(header).put(body).flip();
        unwrap(record);
    }

    private void unwrap(ByteBuffer record) throws IOException {
        do {
            final ByteBuffer decoded = ByteBuffer.allocate(engine.getSession().getApplicationBufferSize());
            final SSLEngineResult result = engine.unwrap(record, decoded);
            if (result.getStatus() == SSLEngineResult.Status.CLOSED) {
                closeNotify = true;
            } else if (result.getStatus() != SSLEngineResult.Status.OK) {
                throw new IOException("Cannot unwrap a complete TLS record: " + result);
            }
            decoded.flip();
            final byte[] bytes = new byte[decoded.remaining()];
            decoded.get(bytes);
            plaintext.writeBytes(bytes);
            runTasks();
            if (record.hasRemaining() && result.bytesConsumed() + result.bytesProduced() == 0) {
                throw new IOException("TLS unwrap made no progress: " + result);
            }
        } while (record.hasRemaining());
    }

    private void runTasks() {
        Runnable task;
        while ((task = engine.getDelegatedTask()) != null) {
            task.run();
        }
    }
}
