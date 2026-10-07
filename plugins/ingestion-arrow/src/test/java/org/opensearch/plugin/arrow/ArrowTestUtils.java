/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.arrow;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;

/** Test-only helper for building Arrow IPC byte streams on the fly rather than from fixture files. */
final class ArrowTestUtils {

    private ArrowTestUtils() {}

    /**
     * Serializes {@code batches} into an Arrow IPC (file format) byte stream, one Arrow record
     * batch per given {@link VectorSchemaRoot}. All batches must share the same schema.
     */
    static byte[] writeIpc(VectorSchemaRoot... batches) throws IOException {
        if (batches.length == 0) {
            throw new IllegalArgumentException("At least one batch is required");
        }
        Schema schema = batches[0].getSchema();
        try (BufferAllocator allocator = new RootAllocator();
                VectorSchemaRoot writerRoot = VectorSchemaRoot.create(schema, allocator)) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (WritableByteChannel channel = Channels.newChannel(out);
                    ArrowFileWriter writer = new ArrowFileWriter(writerRoot, null, channel)) {
                writer.start();
                for (VectorSchemaRoot batch : batches) {
                    // VectorSchemaRoot.create() leaves vectors unallocated (zero-capacity buffers);
                    // VectorSchemaRootAppender needs an already-allocated target to append into.
                    writerRoot.allocateNew();
                    VectorSchemaRootAppender.append(false, writerRoot, batch);
                    writer.writeBatch();
                    writerRoot.clear();
                }
                writer.end();
            }
            return out.toByteArray();
        }
    }
}
