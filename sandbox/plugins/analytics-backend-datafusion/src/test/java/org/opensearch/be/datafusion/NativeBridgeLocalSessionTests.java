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
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.nio.file.Path;
import java.util.List;

/**
 * Smoke test for the coordinator-reduce FFM wrappers added by the datafusion-coordinator-reduce spec.
 *
 * <p>Exercises each new {@link NativeBridge} wrapper against a real native library + global
 * runtime. Mirrors the lifecycle pattern used by {@link DataFusionNativeBridgeTests} — each test
 * creates its own per-test runtime and closes it at the end.
 *
 * <p>Pointer handling follows the plugin convention: raw pointers returned by {@link NativeBridge}
 * are wrapped in {@link org.opensearch.analytics.backend.jni.NativeHandle} subclasses
 * ({@link NativeRuntimeHandle}, {@link DatafusionLocalSession}) so they are registered in the
 * live-handle set that {@link NativeBridge}'s {@code validatePointer} guards check.
 */
public class NativeBridgeLocalSessionTests extends OpenSearchTestCase {

    private NativeRuntimeHandle createRuntime() {
        NativeBridge.initTokioRuntimeManager(2);
        Path spillDir = createTempDir("datafusion-spill");
        long runtimePtr = NativeBridge.createGlobalRuntime(64 * 1024 * 1024, 0L, spillDir.toString(), 32 * 1024 * 1024);
        assertTrue("runtime ptr non-zero", runtimePtr != 0);
        return new NativeRuntimeHandle(runtimePtr);
    }

    private static byte[] schemaIpc(Schema schema) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (WriteChannel channel = new WriteChannel(Channels.newChannel(baos))) {
            MessageSerializer.serialize(channel, schema);
        }
        return baos.toByteArray();
    }

    public void testCreateLocalSessionReturnsNonZeroPtr() {
        NativeRuntimeHandle runtimeHandle = createRuntime();
        try {
            DatafusionLocalSession session = new DatafusionLocalSession(runtimeHandle.get());
            assertTrue("session ptr non-zero", session.getPointer() != 0);
            session.close();
        } finally {
            runtimeHandle.close();
        }
    }

    public void testCloseLocalSessionToleratesZero() {
        // Must not throw.
        NativeBridge.closeLocalSession(0L);
    }

    public void testSenderCloseToleratesZero() {
        NativeBridge.senderClose(0L);
    }

    public void testRegisterPartitionStreamAndSenderClose() throws Exception {
        NativeRuntimeHandle runtimeHandle = createRuntime();
        try {
            DatafusionLocalSession session = new DatafusionLocalSession(runtimeHandle.get());
            try {
                Schema schema = new Schema(List.of(new Field("x", FieldType.nullable(new ArrowType.Int(64, true)), null)));
                long senderPtr = NativeBridge.registerPartitionStream(session.getPointer(), "input-0", schemaIpc(schema));
                assertTrue("sender ptr non-zero", senderPtr != 0);
                NativeBridge.senderClose(senderPtr);
            } finally {
                session.close();
            }
        } finally {
            runtimeHandle.close();
        }
    }
}
