/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.core.common.transport.TransportAddress;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

/**
 * A minimal HTTP/1.1 client over a plain socket, for tests that need to control exactly which bytes are sent when, to
 * half-close the connection, and to tell a clean end-of-stream apart from a connection reset.
 */
@SuppressForbidden(reason = "tests need raw sockets to control segment boundaries, half-closure and resets")
public final class Netty4HttpRawClient implements Closeable {

    public static final int IO_TIMEOUT_MILLIS = 10_000;

    /** Content encodings supported by the HTTP server */
    public enum Encoding {
        IDENTITY(null),
        GZIP("gzip"),
        DEFLATE("deflate");

        public final String header;

        Encoding(String header) {
            this.header = header;
        }

        public byte[] encode(byte[] decoded) {
            if (this == IDENTITY) {
                return decoded;
            }
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (OutputStream encoder = this == GZIP ? new GZIPOutputStream(out) : new DeflaterOutputStream(out, new Deflater())) {
                encoder.write(decoded);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
            return out.toByteArray();
        }
    }

    /** A parsed response: status, headers with lower-case names, and body. */
    public record Response(int status, Map<String, String> headers, byte[] body) {
        public boolean connectionClose() {
            return "close".equalsIgnoreCase(headers.get("connection"));
        }
    }

    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;

    public Netty4HttpRawClient(Socket socket) throws IOException {
        this.socket = socket;
        socket.setSoTimeout(IO_TIMEOUT_MILLIS);
        socket.setTcpNoDelay(true);
        this.in = socket.getInputStream();
        this.out = socket.getOutputStream();
    }

    /** Connects to the IPv4 loopback address the transport is bound to. */
    public static Netty4HttpRawClient connect(TransportAddress[] boundAddresses) throws IOException {
        final Socket socket = new Socket();
        socket.connect(ipv4Address(boundAddresses), IO_TIMEOUT_MILLIS);
        return new Netty4HttpRawClient(socket);
    }

    public static InetSocketAddress ipv4Address(TransportAddress[] boundAddresses) {
        return Arrays.stream(boundAddresses)
            .map(TransportAddress::address)
            .filter(address -> address.getAddress() instanceof Inet4Address)
            .findFirst()
            .orElseThrow(() -> new AssertionError("no IPv4 address in " + Arrays.toString(boundAddresses)));
    }

    public Socket socket() {
        return socket;
    }

    public void write(byte[] bytes) throws IOException {
        write(bytes, 0, bytes.length);
    }

    public void write(byte[] bytes, int offset, int length) throws IOException {
        out.write(bytes, offset, length);
        out.flush();
    }

    /** Writes {@code bytes} in writes of at most {@code writeSize} bytes. */
    public void writeInPieces(byte[] bytes, int writeSize) throws IOException {
        for (int offset = 0; offset < bytes.length; offset += writeSize) {
            write(bytes, offset, Math.min(writeSize, bytes.length - offset));
        }
    }

    public void shutdownOutput() throws IOException {
        socket.shutdownOutput();
    }

    /** Reads one response. Responses are expected to carry a {@code Content-Length}, as all responses tested here do. */
    public Response readResponse() throws IOException {
        final String statusLine = readLine();
        if (statusLine == null) {
            throw new IOException("end of stream before a response");
        }
        final String[] parts = statusLine.split(" ", 3);
        final int status = Integer.parseInt(parts[1]);
        final Map<String, String> headers = new TreeMap<>();
        String line;
        while ((line = readLine()) != null && line.isEmpty() == false) {
            final int colon = line.indexOf(':');
            headers.put(line.substring(0, colon).trim().toLowerCase(Locale.ROOT), line.substring(colon + 1).trim());
        }
        final int length = Integer.parseInt(headers.getOrDefault("content-length", "0"));
        final byte[] body = in.readNBytes(length);
        if (body.length != length) {
            throw new IOException("end of stream within the response body");
        }
        return new Response(status, headers, body);
    }

    /** Returns normally on a clean end-of-stream; fails on unexpected data, and throws on a reset or timeout. */
    public void readEndOfStream() throws IOException {
        final int next = in.read();
        if (next != -1) {
            throw new AssertionError("expected end of stream but got more data, starting with [" + (char) next + "]");
        }
    }

    private String readLine() throws IOException {
        final ByteArrayOutputStream line = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\n') {
                final byte[] bytes = line.toByteArray();
                final int length = bytes.length > 0 && bytes[bytes.length - 1] == '\r' ? bytes.length - 1 : bytes.length;
                return new String(bytes, 0, length, StandardCharsets.ISO_8859_1);
            }
            line.write(b);
        }
        return line.size() == 0 ? null : line.toString(StandardCharsets.ISO_8859_1);
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }

    /** Builds request head and body bytes. */
    public static final class RequestBuilder {
        private final String method;
        private final String path;
        private final Map<String, String> headers = new LinkedHashMap<>();
        private byte[] body = new byte[0];
        private boolean chunked;
        private int chunkSize = 8 * 1024;

        public RequestBuilder(String method, String path) {
            this.method = method;
            this.path = path;
            headers.put("Host", "localhost");
        }

        public RequestBuilder header(String name, String value) {
            headers.put(name, value);
            return this;
        }

        /** Sets the (already encoded) body and the matching {@code Content-Encoding}. */
        public RequestBuilder body(Encoding encoding, byte[] decoded) {
            this.body = encoding.encode(decoded);
            if (encoding.header != null) {
                headers.put("Content-Encoding", encoding.header);
            }
            return this;
        }

        public RequestBuilder rawBody(byte[] encodedBody) {
            this.body = encodedBody;
            return this;
        }

        public RequestBuilder chunked(boolean chunked) {
            this.chunked = chunked;
            return this;
        }

        public int encodedBodyLength() {
            return body.length;
        }

        public byte[] head() {
            final StringBuilder head = new StringBuilder(method).append(' ').append(path).append(" HTTP/1.1\r\n");
            headers.forEach((name, value) -> head.append(name).append(": ").append(value).append("\r\n"));
            if (chunked) {
                head.append("Transfer-Encoding: chunked\r\n");
            } else if (headers.keySet().stream().noneMatch(name -> name.equalsIgnoreCase("Content-Length"))) {
                head.append("Content-Length: ").append(body.length).append("\r\n");
            }
            return head.append("\r\n").toString().getBytes(StandardCharsets.ISO_8859_1);
        }

        /** The body as it goes on the wire, i.e. with chunked framing if enabled. */
        public byte[] wireBody() {
            if (chunked == false) {
                return body;
            }
            final ByteArrayOutputStream wire = new ByteArrayOutputStream();
            for (int offset = 0; offset < body.length; offset += chunkSize) {
                final int length = Math.min(chunkSize, body.length - offset);
                wire.writeBytes((Integer.toHexString(length) + "\r\n").getBytes(StandardCharsets.ISO_8859_1));
                wire.write(body, offset, length);
                wire.writeBytes("\r\n".getBytes(StandardCharsets.ISO_8859_1));
            }
            wire.writeBytes("0\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1));
            return wire.toByteArray();
        }

        public byte[] bytes() {
            final ByteArrayOutputStream wire = new ByteArrayOutputStream();
            wire.writeBytes(head());
            wire.writeBytes(wireBody());
            return wire.toByteArray();
        }
    }
}
