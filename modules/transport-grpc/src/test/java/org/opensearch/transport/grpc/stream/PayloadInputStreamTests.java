/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.stream;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.grpc.spi.StreamPayload;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class PayloadInputStreamTests extends OpenSearchTestCase {

    private final AtomicInteger releases = new AtomicInteger();

    private StreamPayload payload(ByteBuffer... segments) {
        return new StreamPayload(List.of(segments), releases::incrementAndGet);
    }

    private static ByteBuffer direct(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocateDirect(bytes.length).put(bytes).flip();
    }

    private static ByteBuffer heap(String value) {
        return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
    }

    public void testDrainCopiesSegmentsInOrderAndReleasesOnce() throws IOException {
        ByteBuffer directSegment = direct("world");
        StreamPayload payload = payload(heap("hello "), directSegment, ByteBuffer.allocate(0), heap("!"));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (PayloadInputStream stream = new PayloadInputStream(heap(">"), payload)) {
            assertEquals(13, stream.length());
            assertEquals(13, stream.drainTo(out));
            assertEquals(1, releases.get());
        }
        assertEquals(">hello world!", out.toString(StandardCharsets.UTF_8));
        assertEquals("segment positions must not move", 0, directSegment.position());
        assertEquals(1, releases.get());
    }

    public void testDrainCopiesLargeDirectSegmentInChunks() throws IOException {
        byte[] bytes = randomByteArrayOfLength(randomIntBetween(20_000, 40_000));
        ByteBuffer segment = ByteBuffer.allocateDirect(bytes.length).put(bytes).flip();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (PayloadInputStream stream = new PayloadInputStream(ByteBuffer.allocate(0), payload(segment))) {
            stream.drainTo(out);
        }
        assertArrayEquals(bytes, out.toByteArray());
    }

    public void testReadReturnsSameBytesAsDrain() throws IOException {
        StreamPayload payload = payload(heap("ab"), direct("cde"), heap("f"));
        try (PayloadInputStream stream = new PayloadInputStream(heap("0"), payload)) {
            assertEquals('0', stream.read());
            byte[] rest = new byte[16];
            int read = stream.read(rest, 0, rest.length);
            assertEquals("abcdef", new String(rest, 0, read, StandardCharsets.UTF_8));
            assertEquals(-1, stream.read());
            assertEquals(-1, stream.read(rest, 0, rest.length));
            assertEquals(0, stream.read(rest, 0, 0));
            assertEquals("reading keeps the payload until close", 0, releases.get());
        }
        assertEquals(1, releases.get());
    }

    public void testCloseWithoutConsumingReleases() {
        PayloadInputStream stream = new PayloadInputStream(heap("x"), payload(direct("y")));
        stream.close();
        stream.close();
        assertEquals(1, releases.get());
        expectThrows(IllegalStateException.class, () -> stream.drainTo(new ByteArrayOutputStream()));
    }

    public void testPayloadCanOnlyBeConsumedOnce() throws IOException {
        try (PayloadInputStream stream = new PayloadInputStream(heap("x"), payload(heap("y")))) {
            stream.drainTo(new ByteArrayOutputStream());
            expectThrows(IllegalStateException.class, () -> stream.drainTo(new ByteArrayOutputStream()));
            expectThrows(IllegalStateException.class, stream::read);
        }
        assertEquals(1, releases.get());
    }

    public void testReleasesWhenCopyFails() {
        PayloadInputStream stream = new PayloadInputStream(heap("x"), payload(direct("y")));
        OutputStream failing = new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                throw new IOException("boom");
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                throw new IOException("boom");
            }
        };
        expectThrows(IOException.class, () -> stream.drainTo(failing));
        stream.close();
        assertEquals(1, releases.get());
    }
}
