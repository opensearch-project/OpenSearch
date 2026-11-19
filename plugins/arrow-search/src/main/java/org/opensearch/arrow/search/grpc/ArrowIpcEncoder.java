/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search.grpc;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowBuffer;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.transport.grpc.spi.StreamPayload;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;

/**
 * Encodes Arrow data as encapsulated Arrow IPC messages, one per payload. Concatenated, the payloads of a search form an
 * Arrow IPC stream without its optional end-of-stream marker, which any Arrow implementation can read.
 * <p>
 * A record batch payload references the batch's memory instead of copying it. Only the small flatbuffer header is built on
 * the heap. The body segments are views of the batch buffers, laid out and padded exactly as
 * {@link MessageSerializer#serialize(WriteChannel, ArrowRecordBatch)} writes them, and the padding is direct memory too.
 */
public final class ArrowIpcEncoder {

    /** Zeros that pad buffers to 8-byte boundaries. They are direct, so the transport writes a whole batch without copying it. */
    private static final ByteBuffer PADDING = ByteBuffer.allocateDirect(8).asReadOnlyBuffer();

    private ArrowIpcEncoder() {}

    /**
     * Encodes a schema message.
     *
     * @param schema the schema of every batch of the search
     */
    public static StreamPayload schema(Schema schema) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), schema, IpcOption.DEFAULT);
        } catch (IOException e) {
            throw new IllegalStateException("writing to memory cannot fail", e);
        }
        return new StreamPayload(List.of(ByteBuffer.wrap(out.toByteArray())), () -> {});
    }

    /**
     * Encodes a batch as a record batch message and takes ownership of it. Releasing the payload closes the batch. If
     * encoding fails, the batch closes at once.
     *
     * @param root the batch
     */
    public static StreamPayload recordBatch(VectorSchemaRoot root) {
        ArrowRecordBatch batch = null;
        boolean success = false;
        try {
            batch = new VectorUnloader(root).getRecordBatch();
            final ByteBuffer metadata = MessageSerializer.serializeMetadata(batch, IpcOption.DEFAULT);
            final ByteArrayOutputStream header = new ByteArrayOutputStream(metadata.remaining() + 16);
            MessageSerializer.writeMessageBuffer(
                new WriteChannel(Channels.newChannel(header)),
                metadata.remaining(),
                metadata,
                IpcOption.DEFAULT
            );

            final List<ArrowBuf> buffers = batch.getBuffers();
            final List<ArrowBuffer> layout = batch.getBuffersLayout();
            final List<ByteBuffer> segments = new ArrayList<>(2 * buffers.size() + 2);
            segments.add(ByteBuffer.wrap(header.toByteArray()));
            long position = 0;
            for (int i = 0; i < buffers.size(); i++) {
                final ArrowBuffer slot = layout.get(i);
                position = pad(segments, position, slot.getOffset());
                final ArrowBuf buffer = buffers.get(i);
                final long size = buffer.readableBytes();
                if (size != slot.getSize()) {
                    throw new IllegalStateException("buffer " + i + " has " + size + " bytes but its layout reserves " + slot.getSize());
                }
                if (size > 0) {
                    segments.add(buffer.nioBuffer(buffer.readerIndex(), (int) size));
                }
                position += size;
            }
            pad(segments, position, batch.computeBodyLength());

            final ArrowRecordBatch unloaded = batch;
            final StreamPayload payload = new StreamPayload(segments, () -> {
                unloaded.close();
                root.close();
            });
            success = true;
            return payload;
        } catch (IOException e) {
            throw new IllegalStateException("writing to memory cannot fail", e);
        } finally {
            if (success == false) {
                if (batch != null) {
                    batch.close();
                }
                root.close();
            }
        }
    }

    private static long pad(List<ByteBuffer> segments, long position, long target) {
        long gap = target - position;
        while (gap > 0) {
            final int n = (int) Math.min(gap, PADDING.capacity());
            segments.add(PADDING.duplicate().limit(n).slice());
            gap -= n;
        }
        return Math.max(position, target);
    }
}
