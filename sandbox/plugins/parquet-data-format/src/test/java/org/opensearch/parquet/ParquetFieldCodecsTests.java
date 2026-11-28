/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.dataformat.FieldCodec;
import org.opensearch.index.engine.dataformat.FieldStorageParameters;
import org.opensearch.index.mapper.ContentPath;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.FieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.MappingLookup;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.ParametrizedFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ParquetFieldCodecsTests extends OpenSearchTestCase {

    public void testPhysicalEncodingTranslation() {
        ArrowType utf8 = new ArrowType.Utf8();
        ArrowType int64 = new ArrowType.Int(64, true);
        assertEquals("PLAIN", ParquetFieldCodecs.physicalEncoding(FieldCodec.parse("plain"), utf8));
        assertEquals("RLE_DICTIONARY", ParquetFieldCodecs.physicalEncoding(FieldCodec.parse("dictionary"), utf8));
        assertEquals("BYTE_STREAM_SPLIT", ParquetFieldCodecs.physicalEncoding(FieldCodec.parse("byte_split"), int64));
        assertEquals("RLE", ParquetFieldCodecs.physicalEncoding(FieldCodec.parse("rle"), new ArrowType.Bool()));
        // delta is type-directed
        assertEquals("DELTA_BINARY_PACKED", ParquetFieldCodecs.physicalEncoding(FieldCodec.parse("delta"), int64));
        assertEquals("DELTA_BYTE_ARRAY", ParquetFieldCodecs.physicalEncoding(FieldCodec.parse("delta"), utf8));
        assertEquals("DELTA_BYTE_ARRAY", ParquetFieldCodecs.physicalEncoding(FieldCodec.parse("delta"), new ArrowType.Binary()));
        // compression-only codec constrains no encoding
        assertNull(ParquetFieldCodecs.physicalEncoding(FieldCodec.parse("zstd"), utf8));
    }

    public void testPhysicalCompressionTranslation() {
        assertEquals("ZSTD", ParquetFieldCodecs.physicalCompression(FieldCodec.parse("zstd")));
        assertEquals("ZSTD(3)", ParquetFieldCodecs.physicalCompression(FieldCodec.parse("zstd(3)")));
        assertEquals("GZIP(6)", ParquetFieldCodecs.physicalCompression(FieldCodec.parse("gzip(6)")));
        assertEquals("LZ4_RAW", ParquetFieldCodecs.physicalCompression(FieldCodec.parse("lz4")));
        assertEquals("SNAPPY", ParquetFieldCodecs.physicalCompression(FieldCodec.parse("snappy")));
        assertEquals("UNCOMPRESSED", ParquetFieldCodecs.physicalCompression(FieldCodec.parse("none")));
        assertNull(ParquetFieldCodecs.physicalCompression(FieldCodec.parse("delta")));
    }

    public void testValidateForContentTypeAcceptsSupportedCombinations() {
        ParquetFieldCodecs.validateForContentType(FieldCodec.parse(List.of("delta", "zstd(3)")), "long");
        ParquetFieldCodecs.validateForContentType(FieldCodec.parse(List.of("delta", "zstd")), "date");
        ParquetFieldCodecs.validateForContentType(FieldCodec.parse(List.of("delta", "lz4")), "keyword");
        ParquetFieldCodecs.validateForContentType(FieldCodec.parse("dictionary"), "keyword");
        ParquetFieldCodecs.validateForContentType(FieldCodec.parse("byte_split"), "double");
        ParquetFieldCodecs.validateForContentType(FieldCodec.parse("plain"), "text");
        // compression-only codecs are valid for every column type
        ParquetFieldCodecs.validateForContentType(FieldCodec.parse("snappy"), "integer");
        // boolean, ip and binary columns
        ParquetFieldCodecs.validateForContentType(FieldCodec.parse(List.of("rle", "zstd")), "boolean");
        ParquetFieldCodecs.validateForContentType(FieldCodec.parse(List.of("delta", "lz4")), "ip");
        ParquetFieldCodecs.validateForContentType(FieldCodec.parse(List.of("delta", "zstd(3)")), "binary");
        ParquetFieldCodecs.validateForContentType(FieldCodec.parse("plain"), "boolean");
    }

    public void testValidateForContentTypeRejectsUnsupportedEncodingOnBooleanAndBinary() {
        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                () -> ParquetFieldCodecs.validateForContentType(FieldCodec.parse("dictionary"), "boolean")
            ).getMessage(),
            containsString("[dictionary] is not supported for fields of type [boolean]")
        );
        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                () -> ParquetFieldCodecs.validateForContentType(FieldCodec.parse("byte_split"), "ip")
            ).getMessage(),
            containsString("[byte_split] is not supported for fields of type [ip]")
        );
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> ParquetFieldCodecs.validateForContentType(FieldCodec.parse("rle"), "binary"))
                .getMessage(),
            containsString("[rle] is not supported for fields of type [binary]")
        );
    }

    public void testValidateForContentTypeRejectsTokensParquetDoesNotSupport() {
        // Open vocabulary: a token another format might define parses, but Parquet rejects it by name.
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ParquetFieldCodecs.validateForContentType(FieldCodec.parse(List.of("fsst", "zstd")), "keyword")
        );
        assertThat(e.getMessage(), containsString("unsupported codec token [fsst]"));
        assertThat(e.getMessage(), containsString("supported tokens:"));
        // Every well-known token is supported.
        assertTrue(ParquetFieldCodecs.SUPPORTED_TOKENS.containsAll(FieldCodec.ENCODINGS));
        assertTrue(ParquetFieldCodecs.SUPPORTED_TOKENS.containsAll(FieldCodec.COMPRESSIONS));
    }

    public void testValidateForContentTypeRejectsUnsupportedEncoding() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ParquetFieldCodecs.validateForContentType(FieldCodec.parse("byte_split"), "keyword")
        );
        assertThat(e.getMessage(), containsString("codec encoding [byte_split] is not supported for fields of type [keyword]"));

        e = expectThrows(IllegalArgumentException.class, () -> ParquetFieldCodecs.validateForContentType(FieldCodec.parse("rle"), "long"));
        assertThat(e.getMessage(), containsString("[rle] is not supported for fields of type [long]"));

        e = expectThrows(
            IllegalArgumentException.class,
            () -> ParquetFieldCodecs.validateForContentType(FieldCodec.parse("delta"), "no_such_type")
        );
        assertThat(e.getMessage(), containsString("not supported on fields of type [no_such_type]"));
    }

    public void testEncodingForCardinality() {
        ArrowType utf8 = new ArrowType.Utf8();
        ArrowType int64 = new ArrowType.Int(64, true);
        ArrowType f64 = new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
        ArrowType bool = new ArrowType.Bool();
        ArrowType ts = new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, null);

        assertEquals("RLE_DICTIONARY", ParquetFieldCodecs.encodingForCardinality("low", utf8));
        assertEquals("RLE_DICTIONARY", ParquetFieldCodecs.encodingForCardinality("low", int64));
        assertEquals("RLE", ParquetFieldCodecs.encodingForCardinality("low", bool));

        assertEquals("PLAIN", ParquetFieldCodecs.encodingForCardinality("high", utf8));
        assertEquals("PLAIN", ParquetFieldCodecs.encodingForCardinality("high", new ArrowType.Binary()));
        assertEquals("DELTA_BINARY_PACKED", ParquetFieldCodecs.encodingForCardinality("high", int64));
        assertEquals("DELTA_BINARY_PACKED", ParquetFieldCodecs.encodingForCardinality("high", ts));
        assertEquals("BYTE_STREAM_SPLIT", ParquetFieldCodecs.encodingForCardinality("high", f64));
        assertNull("no useful high-cardinality layout for booleans", ParquetFieldCodecs.encodingForCardinality("high", bool));
        assertNull(ParquetFieldCodecs.encodingForCardinality("high", null));
    }

    @SuppressWarnings("deprecation")
    public void testCardinalityHintYieldsToExplicitCodecAndSettings() {
        Settings settings = Settings.builder()
            .putList(ParquetSettings.ENCODING_FIELD_SETTING.getKey(), "legacy_hinted")
            .putList(ParquetSettings.ENCODING_VALUE_SETTING.getKey(), "PLAIN")
            .build();
        FieldMapper hintedOnly = keywordMapperWithCardinality("hinted_only", "low", null);
        FieldMapper codecWins = keywordMapperWithCardinality("codec_wins", "low", FieldCodec.parse("plain"));
        FieldMapper legacyHinted = keywordMapperWithCardinality("legacy_hinted", "low", null);
        FieldMapper highLong = longMapperWithCardinality("high_long", "high");
        MapperService mapperService = mapperService(hintedOnly, codecWins, legacyHinted, highLong);

        ParquetFieldCodecs.FieldStorageConfig config = ParquetFieldCodecs.resolve(settings, mapperService);

        // Hint alone → dictionary + bloom.
        assertEquals("RLE_DICTIONARY", config.encodings().get("hinted_only"));
        assertEquals(Boolean.TRUE, config.bloomFilterEnabled().get("hinted_only"));
        // Explicit codec beats the hint (bloom still defaults on from the low hint).
        assertEquals("PLAIN", config.encodings().get("codec_wins"));
        assertEquals(Boolean.TRUE, config.bloomFilterEnabled().get("codec_wins"));
        // Deprecated explicit setting beats the hint.
        assertEquals("PLAIN", config.encodings().get("legacy_hinted"));
        // high on a long → delta, no bloom.
        assertEquals("DELTA_BINARY_PACKED", config.encodings().get("high_long"));
        assertNull(config.bloomFilterEnabled().get("high_long"));

        assertSettingDeprecationsAndWarnings(
            new Setting<?>[] { ParquetSettings.ENCODING_FIELD_SETTING, ParquetSettings.ENCODING_VALUE_SETTING }
        );
    }

    @SuppressWarnings("deprecation")
    public void testResolveOverlaysMappingOnDeprecatedSettings() {
        // Deprecated per-field settings for three fields.
        Settings settings = Settings.builder()
            .putList(ParquetSettings.ENCODING_FIELD_SETTING.getKey(), "legacy_only", "both")
            .putList(ParquetSettings.ENCODING_VALUE_SETTING.getKey(), "PLAIN", "PLAIN")
            .putList(ParquetSettings.COMPRESSION_FIELD_SETTING.getKey(), "both")
            .putList(ParquetSettings.COMPRESSION_VALUE_SETTING.getKey(), "SNAPPY")
            .putList(ParquetSettings.BLOOM_FILTER_ENABLED_FIELD_SETTING.getKey(), "legacy_only")
            .putList(ParquetSettings.BLOOM_FILTER_ENABLED_VALUE_SETTING.getKey(), "true")
            .build();

        // Mapping: "both" overrides the legacy settings; "mapping_only" is new; "legacy_only" untouched.
        FieldMapper both = longMapper("both", FieldCodec.parse(List.of("delta", "zstd(5)")), false);
        FieldMapper mappingOnly = keywordMapper("mapping_only", FieldCodec.parse("dictionary"), true);
        FieldMapper legacyOnly = longMapper("legacy_only", null, false);
        MapperService mapperService = mapperService(both, mappingOnly, legacyOnly);

        ParquetFieldCodecs.FieldStorageConfig config = ParquetFieldCodecs.resolve(settings, mapperService);

        assertEquals("DELTA_BINARY_PACKED", config.encodings().get("both"));
        assertEquals("ZSTD(5)", config.compressions().get("both"));
        assertEquals("RLE_DICTIONARY", config.encodings().get("mapping_only"));
        assertNull(config.compressions().get("mapping_only"));
        assertEquals(Boolean.TRUE, config.bloomFilterEnabled().get("mapping_only"));
        assertEquals("PLAIN", config.encodings().get("legacy_only"));
        assertEquals(Boolean.TRUE, config.bloomFilterEnabled().get("legacy_only"));
        assertEquals(3, config.encodings().size());
        assertSettingDeprecationsAndWarnings(
            new Setting<?>[] {
                ParquetSettings.ENCODING_FIELD_SETTING,
                ParquetSettings.ENCODING_VALUE_SETTING,
                ParquetSettings.COMPRESSION_FIELD_SETTING,
                ParquetSettings.COMPRESSION_VALUE_SETTING,
                ParquetSettings.BLOOM_FILTER_ENABLED_FIELD_SETTING,
                ParquetSettings.BLOOM_FILTER_ENABLED_VALUE_SETTING }
        );
    }

    @SuppressWarnings("deprecation")
    public void testResolveWithoutMapperServiceIsSettingsOnly() {
        Settings settings = Settings.builder()
            .putList(ParquetSettings.COMPRESSION_FIELD_SETTING.getKey(), "f")
            .putList(ParquetSettings.COMPRESSION_VALUE_SETTING.getKey(), "ZSTD")
            .build();
        ParquetFieldCodecs.FieldStorageConfig config = ParquetFieldCodecs.resolve(settings, null);
        assertEquals(Map.of("f", "ZSTD"), config.compressions());
        assertTrue(config.encodings().isEmpty());
        assertSettingDeprecationsAndWarnings(
            new Setting<?>[] { ParquetSettings.COMPRESSION_FIELD_SETTING, ParquetSettings.COMPRESSION_VALUE_SETTING }
        );
    }

    public void testResolveEmptyWhenNothingDeclared() {
        MapperService mapperService = mapperService(longMapper("plain_field", null, false));
        assertTrue(ParquetFieldCodecs.resolve(Settings.EMPTY, mapperService).isEmpty());
    }

    // --- helpers ---

    private static FieldMapper keywordMapperWithCardinality(String name, String hint, FieldCodec codec) {
        ParametrizedFieldMapper.SharedParameter<FieldCodec> codecParam = FieldStorageParameters.codec(c -> {});
        ParametrizedFieldMapper.SharedParameter<String> hintParam = FieldStorageParameters.cardinality();
        KeywordFieldMapper.Builder builder = new KeywordFieldMapper.Builder(name, null, false, List.of(codecParam, hintParam));
        if (codec != null) {
            codecParam.setValue(codec);
        }
        hintParam.setValue(hint);
        return builder.build(new Mapper.BuilderContext(Settings.EMPTY, new ContentPath(0)));
    }

    private static FieldMapper longMapperWithCardinality(String name, String hint) {
        ParametrizedFieldMapper.SharedParameter<String> hintParam = FieldStorageParameters.cardinality();
        NumberFieldMapper.Builder builder = new NumberFieldMapper.Builder(
            name,
            NumberFieldMapper.NumberType.LONG,
            false,
            false,
            List.of(hintParam)
        );
        hintParam.setValue(hint);
        return builder.build(new Mapper.BuilderContext(Settings.EMPTY, new ContentPath(0)));
    }

    private static FieldMapper longMapper(String name, FieldCodec codec, boolean bloom) {
        ParametrizedFieldMapper.SharedParameter<FieldCodec> codecParam = FieldStorageParameters.codec(c -> {});
        ParametrizedFieldMapper.Parameter<Boolean> bloomParam = FieldStorageParameters.bloomFilter();
        NumberFieldMapper.Builder builder = new NumberFieldMapper.Builder(
            name,
            NumberFieldMapper.NumberType.LONG,
            false,
            false,
            List.of(codecParam, bloomParam)
        );
        if (codec != null) {
            codecParam.setValue(codec);
        }
        bloomParam.setValue(bloom);
        return builder.build(new Mapper.BuilderContext(Settings.EMPTY, new ContentPath(0)));
    }

    private static FieldMapper keywordMapper(String name, FieldCodec codec, boolean bloom) {
        ParametrizedFieldMapper.SharedParameter<FieldCodec> codecParam = FieldStorageParameters.codec(c -> {});
        ParametrizedFieldMapper.Parameter<Boolean> bloomParam = FieldStorageParameters.bloomFilter();
        KeywordFieldMapper.Builder builder = new KeywordFieldMapper.Builder(name, null, false, List.of(codecParam, bloomParam));
        if (codec != null) {
            codecParam.setValue(codec);
        }
        bloomParam.setValue(bloom);
        return builder.build(new Mapper.BuilderContext(Settings.EMPTY, new ContentPath(0)));
    }

    private static MapperService mapperService(FieldMapper... mappers) {
        MappingLookup lookup = new MappingLookup(List.of(mappers), List.of(), List.of(), 0, null);
        DocumentMapper documentMapper = mock(DocumentMapper.class);
        when(documentMapper.mappers()).thenReturn(lookup);
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.documentMapper()).thenReturn(documentMapper);
        for (FieldMapper mapper : mappers) {
            when(mapperService.fieldType(mapper.name())).thenReturn(mapper.fieldType());
        }
        return mapperService;
    }
}
