/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.secure_sm.AccessController;

import javax.net.SocketFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Raw HTTP client that controls HTTP framing and application writes and checks complete responses before EOF.
 */
public final class Netty4HttpRequestSizeTestClient {
    private Netty4HttpRequestSizeTestClient() {}

    static Socket connect(HttpServerTransport transport) throws IOException {
        return connect(transport, SocketFactory.getDefault());
    }

    public static Socket connect(HttpServerTransport transport, SocketFactory socketFactory) throws IOException {
        Socket socket = socketFactory.createSocket();
        socket.setTcpNoDelay(true);
        socket.setSoTimeout(10000);
        try {
            AccessController.doPrivilegedChecked(() -> connectSocket(socket, transport));
        } catch (IOException | RuntimeException e) {
            socket.close();
            throw e;
        }
        return socket;
    }

    @SuppressForbidden(reason = "Raw TCP is required to verify complete HTTP responses before EOF for fragmented uploads")
    private static void connectSocket(Socket socket, HttpServerTransport transport) throws IOException {
        socket.connect(transport.boundAddress().boundAddresses()[0].address());
    }

    public static byte[] bulkBody(int size) {
        String prefix = "{\"index\":{\"_index\":\"oversized\"}}\n{\"field\":\"";
        String suffix = "\"}\n";
        return (prefix + "a".repeat(size - prefix.length() - suffix.length()) + suffix).getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] encode(byte[] body, String encoding) throws IOException {
        if (encoding.equals("identity")) {
            return body;
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (OutputStream compressor = encoding.equals("gzip") ? new GZIPOutputStream(bytes) : new DeflaterOutputStream(bytes)) {
            compressor.write(body);
        }
        return bytes.toByteArray();
    }

    static byte[] headers(int length, String encoding, boolean chunked, boolean close) {
        return ("POST /_bulk HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-ndjson\r\n"
            + (close ? "Connection: close\r\n" : "")
            + (encoding.equals("identity") ? "" : "Content-Encoding: " + encoding + "\r\n")
            + (chunked ? "Transfer-Encoding: chunked\r\n" : "Content-Length: " + length + "\r\n")
            + "\r\n").getBytes(StandardCharsets.US_ASCII);
    }

    public static byte[] request(byte[] body, String encoding, boolean chunked, boolean close) throws IOException {
        return request(body, encoding, chunked, close, 31);
    }

    static byte[] request(byte[] body, String encoding, boolean chunked, boolean close, int chunkSize) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        bytes.write(headers(body.length, encoding, chunked, close));
        if (chunked) {
            for (int offset = 0; offset < body.length; offset += chunkSize) {
                int length = Math.min(chunkSize, body.length - offset);
                bytes.write((Integer.toHexString(length) + "\r\n").getBytes(StandardCharsets.US_ASCII));
                bytes.write(body, offset, length);
                bytes.write("\r\n".getBytes(StandardCharsets.US_ASCII));
            }
            bytes.write("0\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
        } else {
            bytes.write(body);
        }
        return bytes.toByteArray();
    }

    public static void send(OutputStream output, byte[] request, int fragmentSize) throws IOException {
        try {
            for (int offset = 0; offset < request.length;) {
                int length = Math.min(fragmentSize, request.length - offset);
                output.write(request, offset, length);
                output.flush();
                offset += length;
            }
        } catch (IOException e) {
            // Early rejection may stop the upload. Read assertions still require the complete response and clean EOF.
        }
    }

    public static byte[] assertResponse(InputStream input, int status, String scenario) throws IOException {
        ByteArrayOutputStream header = new ByteArrayOutputStream();
        String text;
        do {
            int value = input.read();
            assertTrue("EOF before complete response headers: " + scenario + ", received: " + header, value >= 0);
            header.write(value);
            assertTrue("Unbounded response headers: " + scenario, header.size() < 16384);
            text = header.toString(StandardCharsets.US_ASCII);
        } while (text.endsWith("\r\n\r\n") == false);
        assertTrue("Unexpected response: " + scenario + ": " + text, text.startsWith("HTTP/1.1 " + status + " "));
        int contentLength = -1;
        for (String line : text.split("\r\n")) {
            if (line.toLowerCase(Locale.ROOT).startsWith("content-length:")) {
                contentLength = Integer.parseInt(line.substring(line.indexOf(':') + 1).trim());
            }
        }
        assertTrue("Response must have an explicit content length: " + text, contentLength >= 0);
        byte[] body = input.readNBytes(contentLength);
        assertEquals("EOF before complete response body: " + scenario, contentLength, body.length);
        return body;
    }
}
