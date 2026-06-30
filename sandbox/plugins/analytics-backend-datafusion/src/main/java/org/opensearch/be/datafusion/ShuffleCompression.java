/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;

import java.util.Locale;

/**
 * Hash-shuffle IPC compression policy — the single source of truth for the codec used on the
 * shuffle wire.
 *
 * <p>Shuffle ships each producer partition as an Arrow IPC stream blob (see
 * {@link DatafusionPartitionedSink}); the consumer buffers every chunk on heap in
 * {@code ShuffleBufferManager} until both sides report {@code isLast}, then drains (see
 * {@link ShuffleScanHandler}). For a wide fact-table shuffle (TPC-H lineitem) that buffered
 * footprint is what trips the parent circuit breaker before the per-query shuffle budget forces a
 * spill. Compressing the IPC buffers shrinks BOTH the wire payload AND the heap-resident buffered
 * bytes — relieving the breaker — for a compress/decompress cost that is cheap relative to the
 * serialize → ship → buffer → deserialize round-trip it rides on.
 *
 * <p>This uses STANDARD Arrow IPC compression: the bundled {@code arrow-vector} already carries the
 * codec SPI and compression-aware {@code ArrowStreamWriter}/{@code ArrowStreamReader} ctors. Only
 * the WRITER picks a codec — the codec id rides in each IPC message's metadata, so the
 * {@code ArrowStreamReader} auto-detects and decompresses with no codec hint. That makes a
 * mixed-codec (rolling-deploy) cluster decode correctly and makes an algorithm change a one-line
 * writer edit.
 *
 * <p><b>Codec backends.</b> The shared {@link #FACTORY} returns:
 * <ul>
 *   <li>{@code ZSTD} → Arrow's {@code ZstdCompressionCodec} (zstd-jni, native — fast);</li>
 *   <li>{@code LZ4_FRAME} → {@link FastLz4CompressionCodec} (lz4-java, native) — NOT Arrow's bundled
 *       {@code Lz4CompressionCodec}, whose commons-compress pure-Java matcher hangs on large
 *       fact-table buffers (see {@link FastLz4CompressionCodec});</li>
 *   <li>{@code NO_COMPRESSION} → Arrow's no-op codec.</li>
 * </ul>
 * Both produce/consume the standard on-wire formats, so the reader factory decodes any producer's
 * choice (incl. a peer still on Arrow's commons-compress LZ4).
 *
 * <p>Configured by three JVM system properties (sysprop stopgaps mirroring
 * {@link ShuffleScanHandler}'s {@code recv_timeout_ms} until plumbed as proper cluster settings):
 * <ul>
 *   <li>{@code analytics.mpp.shuffle.compress} ({@code true} | {@code false}; default {@code true}) —
 *       master on/off;</li>
 *   <li>{@code analytics.mpp.compression.codec} ({@code zstd} | {@code lz4}; default {@code zstd}) —
 *       which codec when compression is on;</li>
 *   <li>{@code analytics.mpp.compression.zstd.level} (integer; default {@code 1}, matching Spark's
 *       shuffle codec default) — ZSTD level only (LZ4 is level-agnostic).</li>
 * </ul>
 * Only the WRITER reads these; the reader always passes the factory and auto-detects, so it decodes
 * any codec/level a producer chose (mixed-deploy safe).
 *
 * @opensearch.internal
 */
final class ShuffleCompression {

    /**
     * Shared factory; reused for both write (codec-typed) and read (auto-detect) paths. Delegates to
     * Arrow's {@link CommonsCompressionFactory} for ZSTD / NO_COMPRESSION but substitutes the
     * fast {@link FastLz4CompressionCodec} for LZ4_FRAME (Arrow's bundled LZ4 is the pathologically
     * slow commons-compress pure-Java impl — see {@link FastLz4CompressionCodec}).
     */
    static final CompressionCodec.Factory FACTORY = new CompressionCodec.Factory() {
        @Override
        public CompressionCodec createCodec(CompressionUtil.CodecType codecType) {
            if (codecType == CompressionUtil.CodecType.LZ4_FRAME) {
                return new FastLz4CompressionCodec();
            }
            return CommonsCompressionFactory.INSTANCE.createCodec(codecType);
        }

        @Override
        public CompressionCodec createCodec(CompressionUtil.CodecType codecType, int compressionLevel) {
            if (codecType == CompressionUtil.CodecType.LZ4_FRAME) {
                return new FastLz4CompressionCodec();
            }
            return CommonsCompressionFactory.INSTANCE.createCodec(codecType, compressionLevel);
        }
    };

    /** Master on/off, from {@code analytics.mpp.shuffle.compress} (default {@code true}). */
    static final boolean COMPRESS = resolveCompress();

    /**
     * Writer codec from {@code analytics.mpp.compression.codec} (default {@code zstd}). Only consulted
     * when {@link #COMPRESS}; the reader auto-detects regardless.
     */
    static final CompressionUtil.CodecType WRITE_CODEC = resolveWriteCodec();

    /**
     * ZSTD compression level from {@code analytics.mpp.compression.zstd.level} (default {@code 1},
     * matching Spark's shuffle codec). Only ZSTD consumes it (our {@link FastLz4CompressionCodec} is
     * level-agnostic). Default 1 measured ~28% faster end-to-end on q7 vs Arrow's default 3 (faster
     * consumer decompress + less GC) at comparable heap relief. Passed to the writer's 7-arg ctor as
     * an {@code Optional<Integer>}; empty → Arrow's codec default
     * ({@code ZstdCompressionCodec.DEFAULT_COMPRESSION_LEVEL} = 3).
     */
    static final java.util.Optional<Integer> ZSTD_LEVEL = resolveZstdLevel();

    private ShuffleCompression() {}

    private static boolean resolveCompress() {
        return Boolean.parseBoolean(System.getProperty("analytics.mpp.shuffle.compress", "true").trim());
    }

    private static java.util.Optional<Integer> resolveZstdLevel() {
        String v = System.getProperty("analytics.mpp.compression.zstd.level", "1").trim();
        if (v.isEmpty()) {
            return java.util.Optional.empty();
        }
        try {
            return java.util.Optional.of(Integer.valueOf(v));
        } catch (NumberFormatException e) {
            return java.util.Optional.empty();
        }
    }

    private static CompressionUtil.CodecType resolveWriteCodec() {
        String v = System.getProperty("analytics.mpp.compression.codec", "zstd").trim().toLowerCase(Locale.ROOT);
        switch (v) {
            case "lz4":
            case "lz4_frame":
                return CompressionUtil.CodecType.LZ4_FRAME;
            case "zstd":
            default:
                return CompressionUtil.CodecType.ZSTD;
        }
    }

    /** True when shuffle chunks should be written compressed. */
    static boolean writeCompressed() {
        return COMPRESS;
    }
}
