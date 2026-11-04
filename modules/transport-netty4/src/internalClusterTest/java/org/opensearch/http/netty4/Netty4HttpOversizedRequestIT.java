/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4;

import org.opensearch.OpenSearchNetty4IntegTestCase;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.http.HttpTransportSettings;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

import static org.hamcrest.Matchers.empty;

/**
 * End-to-end checks that a request whose (decoded) body exceeds {@code http.max_content_length} is always answered with a
 * {@code 413} that the client can actually read, regardless of content encoding, framing, keep-alive and how the request
 * bytes are split into TCP segments.
 * <p>
 * The test talks raw HTTP/1.1 over a plain {@link Socket} so that it fully controls segment boundaries and timing. The
 * interesting case is a client (for example a load balancer forwarding a large compressed {@code _bulk}) that is still
 * transmitting the remainder of the body when the server decides to reject the request. If the server fully closes the
 * socket at that point, the bytes the client keeps sending are answered with a TCP RST by the server's kernel, and the RST
 * makes the client's kernel throw away the 413 that was already sitting in its receive buffer: the client observes a
 * connection reset (a load balancer synthesizes a 502) instead of the 413. RFC 9112 section 9.6 calls this out explicitly:
 * a server must not fully close while the client may still be sending; it should half-close and keep reading.
 * <p>
 * Each scenario therefore sends the part of the body that is guaranteed to push the decoded size over the limit, waits
 * until the 413 has arrived in the client's receive buffer (without reading it), pauses so the server has done whatever
 * it does to the connection, and only then sends the rest of the body. That makes the failure deterministic instead of
 * timing dependent: the server either keeps draining (and the client reads 413 followed by a clean FIN) or it has closed
 * and the client gets reset.
 */
@ClusterScope(scope = Scope.SUITE, supportsDedicatedMasters = false, numClientNodes = 0, numDataNodes = 1)
public class Netty4HttpOversizedRequestIT extends OpenSearchNetty4IntegTestCase {

    private static final int LIMIT = 64 * 1024;
    private static final int IO_TIMEOUT_MILLIS = 10_000;

    enum Encoding {
        IDENTITY(null),
        GZIP("gzip"),
        DEFLATE("deflate");

        final String header;

        Encoding(String header) {
            this.header = header;
        }
    }

    enum Framing {
        CONTENT_LENGTH,
        CHUNKED
    }

    /**
     * How the first part of the body (the part that makes the request oversized) ends relative to the point where the
     * decoded size first exceeds the limit.
     */
    enum Split {
        /** the first part ends exactly where the decoded body crosses the limit */
        AT_LIMIT(0),
        /** one extra byte of the body is in the same write as the byte that crosses the limit */
        AT_LIMIT_PLUS_ONE(1),
        /** a few KB more than needed are in the same write, so the server has unread data when it rejects */
        AT_LIMIT_PLUS_MANY(8 * 1024);

        final int extra;

        Split(int extra) {
            this.extra = extra;
        }
    }

    record Scenario(Encoding encoding, Framing framing, boolean connectionClose, boolean expectContinue, Split split, int segmentSize) {
        @Override
        public String toString() {
            return String.format(
                Locale.ROOT,
                "[%s/%s%s%s split=%s segment=%d]",
                encoding,
                framing,
                connectionClose ? "/Connection:close" : "/keep-alive",
                expectContinue ? "/Expect:100-continue" : "",
                split,
                segmentSize
            );
        }
    }

    /** The encoded request body and the encoded offset by which the decoded size is guaranteed to exceed {@link #LIMIT}. */
    record Body(byte[] bytes, int triggerOffset) {
    }

    /** A request on the wire, plus the wire offset of the end of the headers and of the oversize trigger point. */
    record Wire(byte[] bytes, int headersEnd, int triggerOffset) {
    }

    record Response(int status, Map<String, String> headers) {
        boolean connectionClose() {
            return "close".equalsIgnoreCase(headers.get("connection"));
        }
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(HttpTransportSettings.SETTING_HTTP_MAX_CONTENT_LENGTH.getKey(), LIMIT + "b")
            .build();
    }

    /**
     * Deterministic variant: the remainder of the body is only sent after the server has answered, so a server that closes
     * the connection (instead of half-closing and draining) reliably resets the client.
     */
    public void testOversizedRequestIsAlwaysAnsweredWith413BeforeFin() throws Exception {
        final List<String> failures = new ArrayList<>();
        int executed = 0;
        for (Scenario scenario : scenarios()) {
            executed++;
            final String failure = runScenario(scenario, true);
            if (failure != null) {
                failures.add(scenario + " " + failure);
            }
        }
        logger.info("ran [{}] oversized request scenarios, [{}] failed:\n{}", executed, failures.size(), String.join("\n", failures));
        assertThat("oversized request scenarios that did not cleanly receive a 413", failures, empty());
    }

    /**
     * Realistic variant: the client streams the whole request as fast as it can and only then reads, like a proxy would.
     * Whether this fails against a server that fully closes the connection depends on timing, so every scenario is repeated.
     */
    public void testOversizedRequestStreamedWithoutPausingIsAnsweredWith413() throws Exception {
        final List<String> failures = new ArrayList<>();
        int executed = 0;
        for (int iteration = 0; iteration < 5; iteration++) {
            for (Scenario scenario : scenarios()) {
                executed++;
                final String failure = runScenario(scenario, false);
                if (failure != null) {
                    failures.add("iteration " + iteration + " " + scenario + " " + failure);
                }
            }
        }
        logger.info("ran [{}] streamed oversized requests, [{}] failed:\n{}", executed, failures.size(), String.join("\n", failures));
        assertThat("streamed oversized requests that did not cleanly receive a 413", failures, empty());
    }

    /**
     * The server drains a rejected connection only for a bounded time: a client that neither finishes its request nor closes
     * the connection after reading the 413 must not keep the connection open forever.
     */
    public void testLingeringCloseIsBoundedForIdleClients() throws Exception {
        final Body body = bulkBody(Encoding.GZIP, 4 * LIMIT);
        final Wire wire = wire(Encoding.GZIP, Framing.CHUNKED, false, false, body, 8 * 1024);
        try (Socket socket = connect()) {
            final OutputStream out = socket.getOutputStream();
            final InputStream in = socket.getInputStream();
            out.write(wire.bytes(), 0, wire.triggerOffset());
            out.flush();
            final Response response = readResponse(in);
            assertEquals(413, response.status());
            assertTrue("413 must announce the close", response.connectionClose());
            assertEquals("FIN must follow the 413", -1, in.read());

            // stay silent: the server must give up draining once the idle timeout expired and close its side, after which
            // our writes are answered with a reset
            Thread.sleep(Netty4HttpObjectAggregator.LINGER_IDLE_TIMEOUT_MILLIS + 1_000);
            final IOException e = expectThrows(IOException.class, () -> {
                for (int i = 0; i < 10; i++) {
                    out.write(wire.bytes(), wire.triggerOffset(), 16);
                    out.flush();
                    Thread.sleep(50);
                }
            });
            logger.info("write after the linger timeout failed as expected: {}", e.toString());
        }
    }

    /** Sanity check that requests below the limit (compressed or not) still work and keep the connection usable. */
    public void testRequestsBelowLimitStillSucceed() throws Exception {
        for (Encoding encoding : Encoding.values()) {
            for (Framing framing : Framing.values()) {
                final Body body = bulkBody(encoding, LIMIT / 2);
                final Wire wire = wire(encoding, framing, false, false, body, 1024);
                try (Socket socket = connect()) {
                    final OutputStream out = socket.getOutputStream();
                    final InputStream in = socket.getInputStream();
                    out.write(wire.bytes());
                    out.flush();
                    final Response response = readResponse(in);
                    assertEquals(encoding + "/" + framing, 200, response.status());
                    assertFollowUpRequestSucceeds(socket);
                }
            }
        }
    }

    private static List<Scenario> scenarios() {
        final List<Scenario> scenarios = new ArrayList<>();
        for (Encoding encoding : Encoding.values()) {
            for (Framing framing : Framing.values()) {
                for (boolean connectionClose : new boolean[] { false, true }) {
                    for (boolean expectContinue : new boolean[] { false, true }) {
                        if (encoding == Encoding.IDENTITY && framing == Framing.CONTENT_LENGTH && expectContinue) {
                            // the server answers 413 instead of 100 and the client must not send the body, nothing to race
                            continue;
                        }
                        for (Split split : Split.values()) {
                            for (int segmentSize : new int[] { 1460, 16 * 1024 }) {
                                scenarios.add(new Scenario(encoding, framing, connectionClose, expectContinue, split, segmentSize));
                            }
                        }
                    }
                }
            }
        }
        return scenarios;
    }

    /**
     * Runs one scenario and returns a description of what went wrong, or {@code null} if the client cleanly received the 413
     * and the connection was either cleanly closed (FIN after the 413) or is still usable.
     */
    private String runScenario(Scenario scenario, boolean pauseBeforeRemainder) throws Exception {
        final Body body = bulkBody(scenario.encoding(), 4 * LIMIT);
        final Wire wire = wire(
            scenario.encoding(),
            scenario.framing(),
            scenario.connectionClose(),
            scenario.expectContinue(),
            body,
            8 * 1024
        );
        final int firstPartEnd = Math.min(wire.bytes().length, wire.triggerOffset() + scenario.split().extra);
        String stage = "connecting";
        try (Socket socket = connect()) {
            final OutputStream out = socket.getOutputStream();
            final InputStream in = socket.getInputStream();

            stage = "sending headers";
            send(out, wire.bytes(), 0, wire.headersEnd(), scenario.segmentSize(), 0);
            if (scenario.expectContinue()) {
                stage = "waiting for 100 Continue";
                final Response interim = readResponse(in);
                if (interim.status() != 100) {
                    return "expected 100 Continue but got " + interim.status();
                }
            }

            stage = "sending the body up to the point where it exceeds the limit";
            send(out, wire.bytes(), wire.headersEnd(), firstPartEnd, scenario.segmentSize(), 0);

            if (pauseBeforeRemainder) {
                stage = "waiting for the server to respond";
                // peek: wait until the response is in our receive buffer without consuming it, then give the server time
                // to do whatever it does with the connection after it wrote the response
                final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(IO_TIMEOUT_MILLIS);
                while (in.available() == 0) {
                    if (System.nanoTime() > deadline) {
                        return "no response within " + IO_TIMEOUT_MILLIS + "ms";
                    }
                    Thread.sleep(5);
                }
                Thread.sleep(200);
            }

            stage = "sending the remainder of the body";
            if (pauseBeforeRemainder) {
                // pause after each of the first few segments: against a server that closed the connection the first segment
                // is answered with a RST, which makes one of the following writes fail; the rest can go out at full speed
                int offset = firstPartEnd;
                for (int i = 0; i < 5 && offset < wire.bytes().length; i++) {
                    final int end = Math.min(wire.bytes().length, offset + scenario.segmentSize());
                    send(out, wire.bytes(), offset, end, scenario.segmentSize(), 20);
                    offset = end;
                }
                send(out, wire.bytes(), offset, wire.bytes().length, scenario.segmentSize(), 0);
            } else {
                send(out, wire.bytes(), firstPartEnd, wire.bytes().length, scenario.segmentSize(), 0);
            }

            stage = "reading the response";
            final Response response = readResponse(in);
            if (response.status() != 413) {
                return "expected 413 but got " + response.status() + " " + response.headers();
            }
            if (response.connectionClose()) {
                stage = "waiting for FIN after the 413";
                final int next = in.read();
                if (next != -1) {
                    return "expected the connection to be closed after a 413 with [Connection: close], but read [" + next + "]";
                }
            } else {
                stage = "reusing the keep-alive connection after the 413";
                assertFollowUpRequestSucceeds(socket);
            }
            return null;
        } catch (IOException | AssertionError e) {
            return "failed while " + stage + ": " + e;
        }
    }

    @SuppressForbidden(reason = "a raw socket is needed to control how the request is split into TCP segments")
    private Socket connect() throws IOException {
        final HttpServerTransport httpServerTransport = internalCluster().getInstance(HttpServerTransport.class);
        // the test security agent only lets the test framework connect to trusted (local interface) hosts, and it matches
        // them by their string form, so use the IPv4 loopback whose string form is unambiguous
        final TransportAddress transportAddress = Arrays.stream(httpServerTransport.boundAddress().boundAddresses())
            .filter(a -> a.address().getAddress() instanceof Inet4Address)
            .findFirst()
            .orElseThrow(() -> new AssertionError("http is not bound to an IPv4 address"));
        final InetSocketAddress address = transportAddress.address();
        final Socket socket = new Socket();
        socket.setTcpNoDelay(true);
        socket.setSoTimeout(IO_TIMEOUT_MILLIS);
        socket.connect(address, IO_TIMEOUT_MILLIS);
        return socket;
    }

    private static void send(OutputStream out, byte[] bytes, int from, int to, int segmentSize, long pauseMillis) throws Exception {
        for (int offset = from; offset < to; offset += segmentSize) {
            out.write(bytes, offset, Math.min(segmentSize, to - offset));
            out.flush();
            if (pauseMillis > 0) {
                Thread.sleep(pauseMillis);
            }
        }
    }

    private static void assertFollowUpRequestSucceeds(Socket socket) throws IOException {
        final OutputStream out = socket.getOutputStream();
        out.write("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
        out.flush();
        final Response response = readResponse(socket.getInputStream());
        assertEquals("follow-up request on the same connection", 200, response.status());
    }

    /** Reads one HTTP/1.1 response (status line, headers and a Content-Length delimited body) byte by byte. */
    private static Response readResponse(InputStream in) throws IOException {
        final String head = readHead(in);
        final String[] lines = head.split("\r\n");
        final String[] statusLine = lines[0].split(" ", 3);
        if (statusLine.length < 2 || statusLine[0].startsWith("HTTP/") == false) {
            throw new IOException("malformed status line [" + lines[0] + "]");
        }
        final Map<String, String> headers = new TreeMap<>();
        for (int i = 1; i < lines.length; i++) {
            final int colon = lines[i].indexOf(':');
            headers.put(lines[i].substring(0, colon).trim().toLowerCase(Locale.ROOT), lines[i].substring(colon + 1).trim());
        }
        final int status = Integer.parseInt(statusLine[1]);
        if (status >= 200) {
            final long contentLength = Long.parseLong(headers.getOrDefault("content-length", "0"));
            for (long i = 0; i < contentLength; i++) {
                if (in.read() == -1) {
                    throw new IOException("unexpected EOF in response body");
                }
            }
        }
        return new Response(status, headers);
    }

    private static String readHead(InputStream in) throws IOException {
        final ByteArrayOutputStream head = new ByteArrayOutputStream();
        int matched = 0;
        final byte[] terminator = "\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
        while (matched < terminator.length) {
            final int b = in.read();
            if (b == -1) {
                throw new IOException(
                    "connection closed before a complete response was received, got [" + head.toString(StandardCharsets.US_ASCII) + "]"
                );
            }
            head.write(b);
            matched = b == terminator[matched] ? matched + 1 : (b == terminator[0] ? 1 : 0);
        }
        return head.toString(StandardCharsets.US_ASCII).trim();
    }

    /**
     * Builds an (optionally compressed) bulk body. Compressed streams are sync-flushed after every document so the server
     * can decode everything sent so far, which lets us compute the exact encoded offset after which the decoded body is
     * guaranteed to exceed the limit.
     */
    private static Body bulkBody(Encoding encoding, int decodedSize) throws IOException {
        final ByteArrayOutputStream sink = new ByteArrayOutputStream();
        final OutputStream out = switch (encoding) {
            case IDENTITY -> sink;
            case GZIP -> new GZIPOutputStream(sink, 8192, true);
            case DEFLATE -> new DeflaterOutputStream(sink, new Deflater(), 8192, true);
        };
        int decoded = 0;
        int trigger = -1;
        for (int doc = 0; decoded < decodedSize; doc++) {
            final byte[] bytes = ("{\"index\":{\"_index\":\"oversized\",\"_id\":\""
                + doc
                + "\"}}\n{\"field\":\""
                + randomAlphaOfLength(64)
                + " "
                + "lorem ipsum dolor sit amet ".repeat(16)
                + randomAlphaOfLength(64)
                + "\"}\n").getBytes(StandardCharsets.UTF_8);
            out.write(bytes);
            out.flush();
            if (trigger < 0 && decoded + bytes.length > LIMIT) {
                // for identity the very first byte above the limit triggers, compressed streams need the whole flushed block
                trigger = encoding == Encoding.IDENTITY ? LIMIT + 1 : sink.size();
            }
            decoded += bytes.length;
        }
        out.close();
        return new Body(sink.toByteArray(), trigger);
    }

    private static Wire wire(
        Encoding encoding,
        Framing framing,
        boolean connectionClose,
        boolean expectContinue,
        Body body,
        int chunkSize
    ) {
        final StringBuilder head = new StringBuilder("POST /_bulk HTTP/1.1\r\n").append("Host: localhost\r\n")
            .append("Content-Type: application/x-ndjson\r\n");
        if (encoding.header != null) {
            head.append("Content-Encoding: ").append(encoding.header).append("\r\n");
        }
        if (framing == Framing.CONTENT_LENGTH) {
            head.append("Content-Length: ").append(body.bytes().length).append("\r\n");
        } else {
            head.append("Transfer-Encoding: chunked\r\n");
        }
        if (connectionClose) {
            head.append("Connection: close\r\n");
        }
        if (expectContinue) {
            head.append("Expect: 100-continue\r\n");
        }
        head.append("\r\n");

        final ByteArrayOutputStream wire = new ByteArrayOutputStream();
        final byte[] headBytes = head.toString().getBytes(StandardCharsets.US_ASCII);
        wire.writeBytes(headBytes);
        final int headersEnd = wire.size();
        int triggerOffset = -1;
        if (framing == Framing.CONTENT_LENGTH) {
            wire.writeBytes(body.bytes());
            triggerOffset = headersEnd + body.triggerOffset();
        } else {
            for (int offset = 0; offset < body.bytes().length; offset += chunkSize) {
                final int length = Math.min(chunkSize, body.bytes().length - offset);
                wire.writeBytes((Integer.toHexString(length) + "\r\n").getBytes(StandardCharsets.US_ASCII));
                if (triggerOffset < 0 && body.triggerOffset() <= offset + length) {
                    triggerOffset = wire.size() + (body.triggerOffset() - offset);
                }
                wire.write(body.bytes(), offset, length);
                wire.writeBytes("\r\n".getBytes(StandardCharsets.US_ASCII));
            }
            wire.writeBytes("0\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
        }
        return new Wire(wire.toByteArray(), headersEnd, triggerOffset);
    }
}
