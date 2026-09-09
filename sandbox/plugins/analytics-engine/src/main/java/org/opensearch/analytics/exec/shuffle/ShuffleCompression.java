/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.shuffle;

import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.opensearch.analytics.AnalyticsSettings;
import org.opensearch.common.settings.ClusterSettings;

import java.util.Locale;

/**
 * Hash-shuffle IPC compression policy — the single source of truth for the codec used on the
 * shuffle wire.
 *
 * <p>Shuffle ships each producer partition as an Arrow IPC stream blob (the DataFusion backend's
 * {@code DatafusionPartitionedSink}); the consumer buffers every chunk on heap in
 * {@link ShuffleBufferManager} until both sides report {@code isLast}, then drains (the backend's
 * {@code ShuffleScanHandler}). For a wide fact-table shuffle (TPC-H lineitem) that buffered
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
 * <p>Configured by three {@code NodeScope + Dynamic} cluster settings, resolved per-query into a
 * {@link Config} at sink construction ({@link Config#from(ClusterSettings)}):
 * <ul>
 *   <li>{@link AnalyticsSettings#MPP_SHUFFLE_COMPRESS} {@code analytics.mpp.shuffle.compress}
 *       (default {@code false}) — enable only for memory-constrained clusters that cannot fit even a
 *       pruned shuffle on heap and prefer heap headroom over latency;</li>
 *   <li>{@link AnalyticsSettings#MPP_COMPRESSION_CODEC} {@code analytics.mpp.compression.codec}
 *       ({@code zstd} | {@code lz4}; default {@code zstd}) — which codec when compression is on;</li>
 *   <li>{@link AnalyticsSettings#MPP_COMPRESSION_ZSTD_LEVEL} {@code analytics.mpp.compression.zstd.level}
 *       (default {@code 1}, matching Spark's shuffle codec) — ZSTD level only (LZ4 is level-agnostic).</li>
 * </ul>
 * Only the WRITER reads these; the reader always passes the factory and auto-detects, so it decodes
 * any codec/level a producer chose (mixed-setting / rolling-restart safe).
 *
 * @opensearch.internal
 */
public final class ShuffleCompression {

    /**
     * Shared factory; reused for both write (codec-typed) and read (auto-detect) paths. Delegates to
     * Arrow's {@link CommonsCompressionFactory} for ZSTD / NO_COMPRESSION but substitutes the
     * fast {@link FastLz4CompressionCodec} for LZ4_FRAME (Arrow's bundled LZ4 is the pathologically
     * slow commons-compress pure-Java impl — see {@link FastLz4CompressionCodec}).
     */
    public static final CompressionCodec.Factory FACTORY = new CompressionCodec.Factory() {
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

    private ShuffleCompression() {}

    /**
     * Per-query resolved writer policy: whether to compress, which codec, and (for ZSTD) the level.
     * Resolved at sink construction from the LIVE cluster settings ({@link ClusterSettings#get}, so a
     * dynamic {@code PUT /_cluster/settings} update is honored) — the values are the
     * {@code analytics.mpp.shuffle.compress} / {@code analytics.mpp.compression.codec} /
     * {@code analytics.mpp.compression.zstd.level} settings — then handed to the producer sink. The
     * reader never needs this — it auto-detects the codec from the IPC metadata via {@link #FACTORY}.
     */
    public record Config(boolean compress, CompressionUtil.CodecType codec, java.util.Optional<Integer> zstdLevel) {

        /** Off — the sink writes plain (uncompressed) IPC. Used by tests / when no cluster settings is handy. */
        public static final Config DISABLED = new Config(false, CompressionUtil.CodecType.NO_COMPRESSION, java.util.Optional.empty());

        /** Resolve the live values from the cluster settings ({@link AnalyticsSettings}). */
        public static Config from(ClusterSettings clusterSettings) {
            if (!clusterSettings.get(AnalyticsSettings.MPP_SHUFFLE_COMPRESS)) {
                return DISABLED;
            }
            String codecName = clusterSettings.get(AnalyticsSettings.MPP_COMPRESSION_CODEC).trim().toLowerCase(Locale.ROOT);
            CompressionUtil.CodecType codec = ("lz4".equals(codecName) || "lz4_frame".equals(codecName))
                ? CompressionUtil.CodecType.LZ4_FRAME
                : CompressionUtil.CodecType.ZSTD;
            java.util.Optional<Integer> level = codec == CompressionUtil.CodecType.ZSTD
                ? java.util.Optional.of(clusterSettings.get(AnalyticsSettings.MPP_COMPRESSION_ZSTD_LEVEL))
                : java.util.Optional.empty();
            return new Config(true, codec, level);
        }
    }
}
