/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.internal.httpclient;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Flow;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import reactor.adapter.JdkFlowAdapter;
import reactor.core.publisher.Mono;

/**
 * Helper class to deal with request / response bodies.
 */
final class BodyUtils {
    static Mono<?> getBody(HttpRequest httpRequest) {
        return httpRequest.bodyPublisher().map(JdkFlowAdapter::flowPublisherToFlux).map(Mono::from).orElseGet(Mono::empty);
    }

    static String getBodyAsString(Response response) {
        return getBodyAsString(response.getEntity());
    }

    static Mono<String> getBodyAsString(HttpRequest httpRequest) {
        return httpRequest.bodyPublisher().map(p -> {
            var bodySubscriber = HttpResponse.BodySubscribers.ofString(StandardCharsets.UTF_8);
            var flowSubscriber = new StringSubscriber(bodySubscriber);
            p.subscribe(flowSubscriber);
            return Mono.fromCompletionStage(bodySubscriber.getBody());
        }).orElseGet(Mono::empty);
    }

    static String getBodyAsString(List<ByteBuffer> body) {
        final StringBuilder builder = new StringBuilder();
        if (body != null && body.isEmpty() == false) {
            for (ByteBuffer chunk : body) {
                chunk.mark();
                builder.append(StandardCharsets.UTF_8.decode(chunk).toString());
                chunk.reset();
            }
        }
        return builder.toString();
    }

    static List<ByteBuffer> compress(List<ByteBuffer> body) {
        if (body == null || body.isEmpty()) {
            return body;
        } else {
            body.stream().forEach(ByteBuffer::mark);
            try (ByteBufferOutputStream bbout = new ByteBufferOutputStream(); OutputStream out = new GZIPOutputStream(bbout)) {
                try (WritableByteChannel channel = Channels.newChannel(out)) {
                    for (ByteBuffer buffer : body) {
                        channel.write(buffer);
                    }
                    out.flush();
                }
                return bbout.getBufferList();
            } catch (final IOException ex) {
                throw new UncheckedIOException(ex);
            } finally {
                body.stream().forEach(ByteBuffer::reset);
            }
        }
    }

    static ByteBuffer compress(ByteBuffer body) {
        if (body == null) {
            return body;
        } else {
            body.mark();
            try (ByteBufferOutputStream bbout = new ByteBufferOutputStream(); OutputStream out = new GZIPOutputStream(bbout)) {
                try (WritableByteChannel channel = Channels.newChannel(out)) {
                    channel.write(body);
                    out.flush();
                }

                final List<ByteBuffer> bufferList = bbout.getBufferList();
                if (bufferList.isEmpty() == false) {
                    return bufferList.get(0);
                } else {
                    // We should never end up here
                    return ByteBuffer.allocate(0); /* empty byte buffer */
                }
            } catch (final IOException ex) {
                throw new UncheckedIOException(ex);
            } finally {
                body.reset();
            }
        }
    }

    static ByteBuffer decompress(ByteBuffer body) {
        if (body == null) {
            return body;
        } else {
            body.mark();

            try (ByteBufferInputStream bbin = new ByteBufferInputStream(List.of(body)); InputStream in = new GZIPInputStream(bbin)) {
                return ByteBuffer.wrap(in.readAllBytes());
            } catch (final IOException ex) {
                throw new UncheckedIOException(ex);
            } finally {
                body.reset();
            }
        }
    }

    static List<ByteBuffer> decompress(List<ByteBuffer> body) {
        if (body == null || body.isEmpty()) {
            return body;
        } else {
            body.stream().forEach(ByteBuffer::mark);
            try (ByteBufferInputStream bbin = new ByteBufferInputStream(body); InputStream in = new GZIPInputStream(bbin)) {
                return List.of(ByteBuffer.wrap(in.readAllBytes()));
            } catch (final IOException ex) {
                throw new UncheckedIOException(ex);
            } finally {
                body.stream().forEach(ByteBuffer::reset);
            }
        }
    }

    /**
     * See please https://github.com/justinsb/avro/blob/master/src/java/org/apache/avro/ipc/ByteBufferOutputStream.java
     */
    private final static class ByteBufferOutputStream extends OutputStream {
        private static final int BUFFER_SIZE = 8192;
        private List<ByteBuffer> buffers;

        ByteBufferOutputStream() {
            reset();
        }

        /** Returns all data written and resets the stream to be empty. */
        List<ByteBuffer> getBufferList() {
            List<ByteBuffer> result = buffers;
            reset();
            for (ByteBuffer buffer : result) {
                buffer.flip();
            }
            return result;
        }

        private void reset() {
            buffers = new ArrayList<ByteBuffer>(1);
            buffers.add(ByteBuffer.allocate(BUFFER_SIZE));
        }

        @Override
        public void write(int b) {
            ByteBuffer buffer = buffers.get(buffers.size() - 1);
            if (buffer.remaining() < 1) {
                buffer = ByteBuffer.allocate(BUFFER_SIZE);
                buffers.add(buffer);
            }
            buffer.put((byte) b);
        }

        @Override
        public void write(byte[] b, int off, int len) {
            ByteBuffer buffer = buffers.get(buffers.size() - 1);
            int remaining = buffer.remaining();
            while (len > remaining) {
                buffer.put(b, off, remaining);
                len -= remaining;
                off += remaining;
                buffer = ByteBuffer.allocate(BUFFER_SIZE);
                buffers.add(buffer);
                remaining = buffer.remaining();
            }
            buffer.put(b, off, len);
        }
    }

    /**
     * See please https://github.com/justinsb/avro/blob/master/src/java/org/apache/avro/ipc/ByteBufferInputStream.java
     */
    private static final class ByteBufferInputStream extends InputStream {
        private List<ByteBuffer> buffers;
        private int current;

        ByteBufferInputStream(List<ByteBuffer> buffers) {
            this.buffers = buffers;
        }

        /** @see InputStream#read()
         * @throws EOFException if EOF is reached. */
        @Override
        public int read() throws IOException {
            return getBuffer().get() & 0xff;
        }

        /** @see InputStream#read(byte[], int, int)
         * @throws EOFException if EOF is reached before reading all the bytes. */
        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (len == 0) return 0;
            ByteBuffer buffer = getBuffer();
            int remaining = buffer.remaining();
            if (len > remaining) {
                buffer.get(b, off, remaining);
                return remaining;
            } else {
                buffer.get(b, off, len);
                return len;
            }
        }

        /** Returns the next non-empty buffer.
         * @throws EOFException if EOF is reached before reading all the bytes.
         */
        private ByteBuffer getBuffer() throws IOException {
            while (current < buffers.size()) {
                ByteBuffer buffer = buffers.get(current);
                if (buffer.hasRemaining()) return buffer;
                current++;
            }
            throw new EOFException();
        }
    }

    private static final class StringSubscriber implements Flow.Subscriber<ByteBuffer> {
        final HttpResponse.BodySubscriber<String> delegate;

        StringSubscriber(HttpResponse.BodySubscriber<String> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            delegate.onSubscribe(subscription);
        }

        @Override
        public void onNext(ByteBuffer item) {
            delegate.onNext(List.of(item));
        }

        @Override
        public void onError(Throwable throwable) {
            delegate.onError(throwable);
        }

        @Override
        public void onComplete() {
            delegate.onComplete();
        }
    }
}
