/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;

/**
 * Round-trip tests for the hash-shuffle IPC compression contract: a batch serialized by the
 * producer ({@link DatafusionPartitionedSink}) with a given codec must decode losslessly on the
 * consumer ({@link ShuffleScanHandler}) where the {@link ArrowStreamReader} is given
 * {@link ShuffleCompression#FACTORY} and AUTO-DETECTS the codec from IPC metadata. Pure Arrow IPC,
 * no native deps. Mirrors the exact writer/reader construction the sink and scan handler use.
 */
public class ShuffleCompressionTests extends OpenSearchTestCase {

    private static Schema longSchema() {
        return new Schema(List.of(new Field("x", FieldType.nullable(new ArrowType.Int(64, true)), null)));
    }

    private static VectorSchemaRoot makeBatch(RootAllocator alloc, long[] vals) {
        VectorSchemaRoot root = VectorSchemaRoot.create(longSchema(), alloc);
        BigIntVector v = (BigIntVector) root.getVector("x");
        v.allocateNew(vals.length);
        for (int i = 0; i < vals.length; i++) {
            v.setSafe(i, vals[i]);
        }
        v.setValueCount(vals.length);
        root.setRowCount(vals.length);
        return root;
    }

    /** Serialize exactly as the sink does for a given write-codec. */
    private static byte[] writeIpc(RootAllocator alloc, long[] vals, CompressionUtil.CodecType codec) throws Exception {
        try (VectorSchemaRoot root = makeBatch(alloc, vals)) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ArrowStreamWriter writer = codec == CompressionUtil.CodecType.NO_COMPRESSION
                ? new ArrowStreamWriter(root, null, Channels.newChannel(baos))
                : new ArrowStreamWriter(root, null, Channels.newChannel(baos), IpcOption.DEFAULT, ShuffleCompression.FACTORY, codec);
            try (writer) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
            return baos.toByteArray();
        }
    }

    /** Decode exactly as the scan handler does — factory passed, codec auto-detected. */
    private static List<Long> readIpc(byte[] ipc) throws Exception {
        List<Long> out = new ArrayList<>();
        try (
            RootAllocator readerAlloc = new RootAllocator(Long.MAX_VALUE);
            ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipc), readerAlloc, ShuffleCompression.FACTORY)
        ) {
            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                BigIntVector v = (BigIntVector) root.getVector("x");
                for (int i = 0; i < v.getValueCount(); i++) {
                    out.add(v.get(i));
                }
            }
        }
        return out;
    }

    private void assertRoundTrips(CompressionUtil.CodecType codec) throws Exception {
        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            long[] vals = new long[2048];
            for (int i = 0; i < vals.length; i++) {
                vals[i] = i % 8; // highly compressible — exercises the codec, not just framing
            }
            byte[] ipc = writeIpc(alloc, vals, codec);
            List<Long> decoded = readIpc(ipc);
            assertEquals("row count must survive " + codec, vals.length, decoded.size());
            for (int i = 0; i < vals.length; i++) {
                assertEquals("value[" + i + "] must survive " + codec, vals[i], (long) decoded.get(i));
            }
        }
    }

    public void testZstdRoundTrips() throws Exception {
        assertRoundTrips(CompressionUtil.CodecType.ZSTD);
    }

    public void testLz4RoundTrips() throws Exception {
        assertRoundTrips(CompressionUtil.CodecType.LZ4_FRAME);
    }

    public void testNoCompressionRoundTrips() throws Exception {
        assertRoundTrips(CompressionUtil.CodecType.NO_COMPRESSION);
    }

    /** The reader auto-detects the codec from metadata, so the SAME factory decodes any producer's
     *  choice — the property that makes a mixed-codec (rolling deploy) cluster safe. */
    public void testReaderDecodesAllCodecsWithOneFactory() throws Exception {
        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            long[] vals = { 10L, 20L, 30L, 40L, 50L };
            for (CompressionUtil.CodecType codec : List.of(
                CompressionUtil.CodecType.NO_COMPRESSION,
                CompressionUtil.CodecType.LZ4_FRAME,
                CompressionUtil.CodecType.ZSTD
            )) {
                byte[] ipc = writeIpc(alloc, vals, codec);
                assertEquals("codec " + codec + " must decode via the shared factory", List.of(10L, 20L, 30L, 40L, 50L), readIpc(ipc));
            }
        }
    }

    /** Compression must actually shrink a compressible batch (otherwise it buys no breaker relief). */
    public void testZstdShrinksCompressibleBatch() throws Exception {
        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            long[] vals = new long[8192];
            // all-equal column → near-perfect compression
            byte[] raw = writeIpc(alloc, vals, CompressionUtil.CodecType.NO_COMPRESSION);
            byte[] zstd = writeIpc(alloc, vals, CompressionUtil.CodecType.ZSTD);
            assertTrue(
                "zstd (" + zstd.length + "B) must be smaller than raw (" + raw.length + "B) for a compressible batch",
                zstd.length < raw.length
            );
        }
    }

    /** The configured factory must actually produce a working ZSTD codec (guards the dependency wiring). */
    public void testFactoryProvidesZstdCodec() {
        CompressionCodec codec = ShuffleCompression.FACTORY.createCodec(CompressionUtil.CodecType.ZSTD);
        assertNotNull(codec);
        assertEquals(CompressionUtil.CodecType.ZSTD, codec.getCodecType());
    }
}
