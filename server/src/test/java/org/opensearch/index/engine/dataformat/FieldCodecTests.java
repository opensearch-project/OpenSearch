/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;

public class FieldCodecTests extends OpenSearchTestCase {

    public void testSingleCompressionToken() {
        FieldCodec codec = FieldCodec.parse("zstd");
        assertNull(codec.encoding());
        assertEquals(FieldCodec.ZSTD, codec.compression());
        assertNull(codec.compressionLevel());
        assertEquals("zstd", codec.toMappingValue());
    }

    public void testCompressionWithLevel() {
        FieldCodec codec = FieldCodec.parse("ZSTD(3)");
        assertEquals(FieldCodec.ZSTD, codec.compression());
        assertEquals(Integer.valueOf(3), codec.compressionLevel());
        assertEquals("zstd(3)", codec.toMappingValue());
    }

    public void testEncodingThenCompressionList() {
        FieldCodec codec = FieldCodec.parse(List.of("delta", "zstd(3)"));
        assertEquals(FieldCodec.DELTA, codec.encoding());
        assertEquals(FieldCodec.ZSTD, codec.compression());
        assertEquals(Integer.valueOf(3), codec.compressionLevel());
        assertEquals(List.of("delta", "zstd(3)"), codec.toMappingValue());
        assertEquals("delta,zstd(3)", codec.toString());
    }

    public void testEncodingOnly() {
        FieldCodec codec = FieldCodec.parse(List.of("dictionary"));
        assertEquals(FieldCodec.DICTIONARY, codec.encoding());
        assertNull(codec.compression());
        assertEquals("dictionary", codec.toMappingValue());
    }

    public void testRoundTripThroughMappingValue() {
        for (Object value : List.of("plain", "lz4", "gzip(9)", List.of("byte_split", "snappy"), List.of("rle", "none"))) {
            FieldCodec parsed = FieldCodec.parse(value);
            assertEquals(parsed, FieldCodec.parse(parsed.toMappingValue()));
        }
    }

    public void testFormatDefinedTokenParsesAndIsRejectedOnlyByRequireSupported() {
        // The vocabulary is open: a token core does not know is parsed and left to the format's validator.
        FieldCodec codec = FieldCodec.parse(List.of("fsst", "zstd(3)"));
        assertEquals(2, codec.tokens().size());
        assertTrue(codec.tokens().get(0).isFormatDefined());
        assertNull("format-defined tokens are not reported as the well-known encoding", codec.encoding());
        assertEquals(FieldCodec.ZSTD, codec.compression());
        assertEquals(List.of("fsst", "zstd(3)"), codec.toMappingValue());

        // A format that does not support it rejects it by name with the supported set.
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> codec.requireSupported(Set.of(FieldCodec.DELTA, FieldCodec.ZSTD))
        );
        assertThat(e.getMessage(), containsString("unsupported codec token [fsst]"));
        assertThat(e.getMessage(), containsString("supported tokens: [delta, zstd]"));

        // A format that does support it accepts the whole codec.
        codec.requireSupported(Set.of("fsst", FieldCodec.ZSTD));

        // Format-defined tokens may carry a level; the format validates it.
        assertEquals(Integer.valueOf(7), FieldCodec.parse("alp(7)").tokens().get(0).level());
    }

    public void testRequireSupportedRejectsUnsupportedWellKnownToken() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FieldCodec.parse("gzip").requireSupported(Set.of(FieldCodec.ZSTD, FieldCodec.LZ4))
        );
        assertThat(e.getMessage(), containsString("unsupported codec token [gzip]"));
    }

    public void testMalformedTokenName() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> FieldCodec.parse("zstd-3"));
        assertThat(e.getMessage(), containsString("malformed codec token [zstd-3]"));
    }

    public void testCompressionBeforeEncodingRejected() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> FieldCodec.parse(List.of("zstd", "delta")));
        assertThat(e.getMessage(), containsString("must precede the compression"));
    }

    public void testDuplicateClassRejected() {
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> FieldCodec.parse(List.of("delta", "plain"))).getMessage(),
            containsString("second encoding")
        );
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> FieldCodec.parse(List.of("zstd", "lz4"))).getMessage(),
            containsString("second compression")
        );
    }

    public void testLevelValidation() {
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> FieldCodec.parse("zstd(23)")).getMessage(),
            containsString("between 1 and 22")
        );
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> FieldCodec.parse("gzip(0)")).getMessage(),
            containsString("between 1 and 9")
        );
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> FieldCodec.parse("lz4(3)")).getMessage(),
            containsString("does not accept a level")
        );
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> FieldCodec.parse("delta(3)")).getMessage(),
            containsString("does not accept a level")
        );
    }

    public void testMalformedValues() {
        expectThrows(IllegalArgumentException.class, () -> FieldCodec.parse(""));
        expectThrows(IllegalArgumentException.class, () -> FieldCodec.parse(List.of()));
        expectThrows(IllegalArgumentException.class, () -> FieldCodec.parse(List.of("delta", "zstd", "lz4")));
        expectThrows(IllegalArgumentException.class, () -> FieldCodec.parse(List.of("delta", 3)));
        expectThrows(IllegalArgumentException.class, () -> FieldCodec.parse(42));
        expectThrows(IllegalArgumentException.class, () -> FieldCodec.parse("zstd(3"));
    }
}
