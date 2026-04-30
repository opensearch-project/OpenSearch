/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;

/**
 * Helper for serializing an Arrow {@link Schema} to the IPC stream bytes expected by the
 * Rust-side {@code df_register_partition_stream} export.
 */
public final class ArrowSchemaIpc {

    private ArrowSchemaIpc() {}

    /**
     * Encodes the schema as a single Arrow IPC stream message containing the schema header.
     *
     * @param schema the Arrow schema
     * @return a heap byte array safe to hand to FFM
     */
    public static byte[] toBytes(Schema schema) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (WriteChannel channel = new WriteChannel(Channels.newChannel(baos))) {
            MessageSerializer.serialize(channel, schema);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize Arrow schema to IPC bytes", e);
        }
        return baos.toByteArray();
    }
}
