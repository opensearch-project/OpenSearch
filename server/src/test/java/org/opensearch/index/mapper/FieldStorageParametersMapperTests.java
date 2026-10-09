/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.Version;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.engine.dataformat.FieldCodec;
import org.opensearch.index.engine.dataformat.FieldStorageParameters;
import org.opensearch.index.mapper.ParametrizedFieldMapper.Parameter;
import org.opensearch.index.mapper.ParametrizedFieldMapper.SharedParameter;
import org.opensearch.index.mapper.ParametrizedFieldMapper.SideEffectParameter;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

/**
 * Verifies that the number and date mappers carry plugin-contributed storage parameters ({@code codec},
 * {@code bloom_filter}) through build, serialization, re-initialisation and merge, the same way the keyword and
 * text mappers already do.
 */
public class FieldStorageParametersMapperTests extends OpenSearchTestCase {

    private static final Mapper.BuilderContext CONTEXT = new Mapper.BuilderContext(Settings.EMPTY, new ContentPath(0));

    public void testNumberMapperCarriesStorageParameters() throws IOException {
        FieldCodec codec = FieldCodec.parse(List.of("delta", "zstd(3)"));
        NumberFieldMapper mapper = numberMapper("count", codec, true);

        assertEquals(codec, mapper.mappingPluginParameterValues().get(FieldStorageParameters.CODEC));
        assertEquals(Boolean.TRUE, mapper.mappingPluginParameterValues().get(FieldStorageParameters.BLOOM_FILTER));

        String json = toJson(mapper);
        assertThat(json, containsString("\"codec\":[\"delta\",\"zstd(3)\"]"));
        assertThat(json, containsString("\"bloom_filter\":true"));

        // The merge builder reads the values back from the built mapper.
        NumberFieldMapper.Builder merge = (NumberFieldMapper.Builder) mapper.getMergeBuilder();
        assertEquals(codec, parameter(merge, FieldStorageParameters.CODEC).getValue());
        assertEquals(Boolean.TRUE, parameter(merge, FieldStorageParameters.BLOOM_FILTER).getValue());
    }

    public void testDateMapperCarriesStorageParameters() throws IOException {
        FieldCodec codec = FieldCodec.parse("zstd");
        DateFieldMapper mapper = dateMapper("@timestamp", codec, false);

        assertEquals(codec, mapper.mappingPluginParameterValues().get(FieldStorageParameters.CODEC));
        String json = toJson(mapper);
        assertThat(json, containsString("\"codec\":\"zstd\""));
        // bloom_filter=false is the default and is not serialized.
        assertThat(json, not(containsString("bloom_filter")));

        DateFieldMapper.Builder merge = (DateFieldMapper.Builder) mapper.getMergeBuilder();
        assertEquals(codec, parameter(merge, FieldStorageParameters.CODEC).getValue());
    }

    public void testBooleanIpAndBinaryMappersCarryStorageParameters() throws IOException {
        FieldCodec rle = FieldCodec.parse(List.of("rle", "zstd"));
        List<Parameter<?>> boolParams = params(rle, true);
        BooleanFieldMapper bool = new BooleanFieldMapper.Builder("flag", Settings.EMPTY, boolParams).build(CONTEXT);
        assertEquals(rle, bool.mappingPluginParameterValues().get(FieldStorageParameters.CODEC));
        assertEquals(Boolean.TRUE, bool.mappingPluginParameterValues().get(FieldStorageParameters.BLOOM_FILTER));
        assertThat(toJson(bool), containsString("\"codec\":[\"rle\",\"zstd\"]"));
        assertEquals(rle, parameter(bool.getMergeBuilder(), FieldStorageParameters.CODEC).getValue());
        assertEquals(Boolean.TRUE, parameter(bool.getMergeBuilder(), FieldStorageParameters.BLOOM_FILTER).getValue());

        FieldCodec delta = FieldCodec.parse(List.of("delta", "lz4"));
        IpFieldMapper ip = new IpFieldMapper.Builder("addr", false, Version.CURRENT, Settings.EMPTY, params(delta, false)).build(CONTEXT);
        assertEquals(delta, ip.mappingPluginParameterValues().get(FieldStorageParameters.CODEC));
        assertThat(toJson(ip), containsString("\"codec\":[\"delta\",\"lz4\"]"));
        assertEquals(delta, parameter(ip.getMergeBuilder(), FieldStorageParameters.CODEC).getValue());

        FieldCodec none = FieldCodec.parse("none");
        BinaryFieldMapper binary = new BinaryFieldMapper.Builder("blob", false, params(none, false)).build(CONTEXT);
        assertEquals(none, binary.mappingPluginParameterValues().get(FieldStorageParameters.CODEC));
        assertThat(toJson(binary), containsString("\"codec\":\"none\""));
        assertEquals(none, parameter(binary.getMergeBuilder(), FieldStorageParameters.CODEC).getValue());
    }

    public void testAbsentCodecLeavesNoTraceInMapping() throws IOException {
        NumberFieldMapper mapper = numberMapper("count", null, false);
        assertNull(mapper.mappingPluginParameterValues().get(FieldStorageParameters.CODEC));
        String json = toJson(mapper);
        assertThat(json, not(containsString("codec")));
        assertThat(json, not(containsString("bloom_filter")));
    }

    public void testCodecIsNotUpdateable() {
        NumberFieldMapper existing = numberMapper("count", FieldCodec.parse("zstd(3)"), false);
        NumberFieldMapper changed = numberMapper("count", FieldCodec.parse("lz4"), false);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> existing.merge(changed));
        assertThat(e.getMessage(), containsString("Cannot update parameter [codec] from [zstd(3)] to [lz4]"));

        // Merging an identical codec is a no-op rather than a conflict.
        NumberFieldMapper same = numberMapper("count", FieldCodec.parse("zstd(3)"), false);
        assertEquals(FieldCodec.parse("zstd(3)"), existing.merge(same).mappingPluginParameterValues().get(FieldStorageParameters.CODEC));
    }

    public void testCardinalityParameterParsesValidatesAndAppliesSideEffect() throws IOException {
        // Values are normalised and restricted to low|high.
        SharedParameter<String> hint = FieldStorageParameters.cardinality();
        Builder builder = new Builder("f", List.of(hint));
        builder.parse("f", null, new HashMap<>(Map.of(FieldStorageParameters.CARDINALITY, "LOW")));
        assertEquals(FieldStorageParameters.CARDINALITY_LOW, hint.getValue());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new Builder("f", List.of(FieldStorageParameters.cardinality())).parse(
                "f",
                null,
                new HashMap<>(Map.of(FieldStorageParameters.CARDINALITY, "medium"))
            )
        );
        assertThat(e.getMessage(), containsString("cardinality must be one of [high, low], got [medium]"));

        // A format's side effect runs at build time with the resolved hint (here: disable indexing for low).
        SharedParameter<String> withEffect = FieldStorageParameters.cardinality(h -> {}, (b, h) -> {
            if (FieldStorageParameters.CARDINALITY_LOW.equals(h)) {
                b.setParameterValue("index", false);
            }
        });
        withEffect.setValue(FieldStorageParameters.CARDINALITY_LOW);
        NumberFieldMapper low = new NumberFieldMapper.Builder("n", NumberFieldMapper.NumberType.LONG, false, false, List.of(withEffect))
            .build(CONTEXT);
        assertEquals("low", low.mappingPluginParameterValues().get(FieldStorageParameters.CARDINALITY));
        assertThat(toJson(low), containsString("\"index\":false"));
        assertThat(toJson(low), containsString("\"cardinality\":\"low\""));

        // high → side effect is a no-op; value still round-trips and is readable by the merge builder.
        SharedParameter<String> high = FieldStorageParameters.cardinality(h -> {}, (b, h) -> {
            if (FieldStorageParameters.CARDINALITY_LOW.equals(h)) {
                b.setParameterValue("index", false);
            }
        });
        high.setValue(FieldStorageParameters.CARDINALITY_HIGH);
        NumberFieldMapper highMapper = new NumberFieldMapper.Builder("n", NumberFieldMapper.NumberType.LONG, false, false, List.of(high))
            .build(CONTEXT);
        assertThat(toJson(highMapper), not(containsString("\"index\":false")));
        assertEquals("high", parameter(highMapper.getMergeBuilder(), FieldStorageParameters.CARDINALITY).getValue());

        // Absent → no trace.
        assertThat(toJson(numberMapper("n", null, false)), not(containsString("cardinality")));
    }

    public void testSharedParameterMergeCombinesSideEffects() {
        java.util.concurrent.atomic.AtomicInteger applied = new java.util.concurrent.atomic.AtomicInteger();
        SharedParameter<String> a = FieldStorageParameters.cardinality(h -> {}, (b, h) -> applied.incrementAndGet());
        SharedParameter<String> b = FieldStorageParameters.cardinality(h -> {}, (bb, h) -> applied.addAndGet(10));
        SharedParameter<String> merged = a.mergeWith(b);
        assertEquals(2, merged.sideEffects().size());
        merged.setValue(FieldStorageParameters.CARDINALITY_HIGH);
        new NumberFieldMapper.Builder("n", NumberFieldMapper.NumberType.LONG, false, false, List.of(merged)).build(CONTEXT);
        assertEquals("both formats' side effects ran once", 11, applied.get());
    }

    public void testSharedParameterMergesValidatorsAcrossFormats() {
        // Two formats contribute `codec`; the merged parameter must satisfy both validators.
        SharedParameter<FieldCodec> formatA = FieldStorageParameters.codec(c -> c.requireSupported(Set.of("delta", "zstd", "lz4")));
        SharedParameter<FieldCodec> formatB = FieldStorageParameters.codec(c -> c.requireSupported(Set.of("delta", "zstd", "fsst")));

        List<Parameter<?>> collected = new ArrayList<>();
        assertTrue(SharedParameter.mergeInto(collected, formatA));
        assertTrue(SharedParameter.mergeInto(collected, formatB));
        assertEquals("same-name shared parameters collapse into one", 1, collected.size());
        SharedParameter<?> merged = (SharedParameter<?>) collected.get(0);
        assertEquals(2, merged.validators().size());
        assertNotSame("merge must produce a fresh instance", formatA, merged);

        Builder builder = new Builder("f", collected);
        // Accepted by both formats.
        builder.parse("f", null, new HashMap<>(Map.of(FieldStorageParameters.CODEC, List.of("delta", "zstd"))));
        assertEquals(FieldCodec.parse(List.of("delta", "zstd")), merged.getValue());

        // lz4 is only supported by format A → rejected by the merged validator.
        Builder other = new Builder("f", List.of(formatA.mergeWith(formatB)));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> other.parse("f", null, new HashMap<>(Map.of(FieldStorageParameters.CODEC, "lz4")))
        );
        assertThat(e.getMessage(), containsString("unsupported codec token [lz4]"));

        // The merged parameter still serializes via the codec mapping value.
        assertEquals(List.of("delta", "zstd"), ((SharedParameter<FieldCodec>) merged).getValue().toMappingValue());
    }

    public void testMergeIntoRefusesNonSharedDuplicates() {
        List<Parameter<?>> collected = new ArrayList<>();
        assertTrue(SharedParameter.mergeInto(collected, SideEffectParameter.boolParam("low_cardinality", false, false, (b, v) -> {})));
        // A second non-shared parameter of the same name is a clash the caller must report.
        assertFalse(SharedParameter.mergeInto(collected, SideEffectParameter.boolParam("low_cardinality", false, false, (b, v) -> {})));
        assertEquals(1, collected.size());
        // A shared parameter cannot merge into a non-shared one of the same name either.
        assertFalse(SharedParameter.mergeInto(collected, new SharedParameter<>("low_cardinality", false, (n, c, o) -> true, List.of())));
        // Different names simply append.
        assertTrue(SharedParameter.mergeInto(collected, FieldStorageParameters.bloomFilter()));
        assertEquals(2, collected.size());
    }

    public void testCodecValidatorIsAppliedByTheFactory() {
        // The plugin-supplied validator is part of the parameter's parser; a rejecting validator surfaces its message.
        SharedParameter<FieldCodec> rejecting = FieldStorageParameters.codec(
            c -> { throw new IllegalArgumentException("rejected by format"); }
        );
        SharedParameter<FieldCodec> accepting = FieldStorageParameters.codec(c -> {});

        Builder rejectingBuilder = new Builder("f", List.of(rejecting));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> rejectingBuilder.parse("f", null, new HashMap<>(Map.of(FieldStorageParameters.CODEC, "rle")))
        );
        assertThat(e.getMessage(), containsString("rejected by format"));

        Builder acceptingBuilder = new Builder("f", List.of(accepting));
        acceptingBuilder.parse("f", null, new HashMap<>(Map.of(FieldStorageParameters.CODEC, List.of("delta", "zstd"))));
        assertEquals(FieldCodec.parse(List.of("delta", "zstd")), accepting.getValue());
    }

    /** Minimal builder exposing {@link ParametrizedFieldMapper.Builder#parse} for the validator test. */
    private static class Builder extends ParametrizedFieldMapper.Builder {
        Builder(String name, List<Parameter<?>> pluginParameters) {
            super(name);
            setPluginMappingParameters(pluginParameters);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return List.copyOf(pluginMappingParameters());
        }

        @Override
        public ParametrizedFieldMapper build(Mapper.BuilderContext context) {
            throw new UnsupportedOperationException();
        }
    }

    // --- helpers ---

    /** Builds a codec + bloom_filter parameter pair with the given values already set. */
    private static List<Parameter<?>> params(FieldCodec codec, boolean bloom) {
        SharedParameter<FieldCodec> codecParam = FieldStorageParameters.codec(c -> {});
        Parameter<Boolean> bloomParam = FieldStorageParameters.bloomFilter();
        if (codec != null) {
            codecParam.setValue(codec);
        }
        if (bloom) {
            bloomParam.setValue(true);
        }
        return List.of(codecParam, bloomParam);
    }

    private static NumberFieldMapper numberMapper(String name, FieldCodec codec, boolean bloom) {
        SharedParameter<FieldCodec> codecParam = FieldStorageParameters.codec(c -> {});
        Parameter<Boolean> bloomParam = FieldStorageParameters.bloomFilter();
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
        if (bloom) {
            bloomParam.setValue(true);
        }
        return builder.build(CONTEXT);
    }

    private static DateFieldMapper dateMapper(String name, FieldCodec codec, boolean bloom) {
        SharedParameter<FieldCodec> codecParam = FieldStorageParameters.codec(c -> {});
        Parameter<Boolean> bloomParam = FieldStorageParameters.bloomFilter();
        DateFieldMapper.Builder builder = new DateFieldMapper.Builder(
            name,
            DateFieldMapper.Resolution.MILLISECONDS,
            null,
            false,
            Version.CURRENT,
            Settings.EMPTY,
            List.of(codecParam, bloomParam)
        );
        if (codec != null) {
            codecParam.setValue(codec);
        }
        if (bloom) {
            bloomParam.setValue(true);
        }
        return builder.build(CONTEXT);
    }

    private static Parameter<?> parameter(ParametrizedFieldMapper.Builder builder, String name) {
        for (Parameter<?> parameter : builder.getParameters()) {
            if (parameter.name.equals(name)) {
                return parameter;
            }
        }
        throw new AssertionError("no parameter [" + name + "]");
    }

    private static String toJson(ParametrizedFieldMapper mapper) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        mapper.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return builder.toString();
    }
}
