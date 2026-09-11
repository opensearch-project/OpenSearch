/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.benchmark;

import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.VersionFieldMapper;
import org.opensearch.parquet.engine.ParquetDataFormat;
import org.opensearch.parquet.writer.FieldValuePair;
import org.opensearch.parquet.writer.ParquetDocumentInput;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Replays, per document, the exact {@link ParquetDocumentInput} call sequence the mapper layer
 * performs while parsing a ClickBench {@code hits} row (106 columns: 25 keyword, 49 short,
 * 19 integer, 6 long, 4 date, in the real column order), plus the four metadata fields.
 *
 * <p>For every keyword field in the default {@code multi_value: auto} state,
 * {@code ParametrizedFieldMapper#addFieldForPluggableFormat} first asks
 * {@link DocumentInput#getFieldCount(String)} whether the field was already seen (to decide on
 * scalar-to-LIST promotion) and only then calls {@link DocumentInput#addField}. The
 * {@code preCheck} param toggles that call so its cost can be isolated from the plain collect path.
 *
 * <p>Run with:
 * <pre>
 * ./gradlew -Dsandbox.enabled=true :sandbox:plugins:parquet-data-format:benchmarks:run \
 *     --args='DocumentInputCollectBenchmark'
 * </pre>
 */
@Fork(1)
@Warmup(iterations = 3, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class DocumentInputCollectBenchmark {

    /** ClickBench hits column types in mapping order: K=keyword S=short I=integer L=long D=date. */
    private static final String CLICKBENCH_COLUMN_SHAPE =
        "SSKKIDISIISSIISDDLISSSKSISSSKISSSSSSSSSSSSSDSKSSSIKKKKKKKSKLKKSLIIISSSIISSKISSSISSKKSKSLIKKKKKSSKLLSSIS";

    /** Whether to run the mapper's promotion pre-check ({@code getFieldCount}) before each keyword value. */
    @Param({ "true", "false" })
    public boolean preCheck;

    private static final DataFormat PARQUET = new ParquetDataFormat();

    private MappedFieldType[] fieldTypes;
    private Object[] values;
    private boolean[] isKeyword;

    private MappedFieldType idType;
    private MappedFieldType seqNoType;
    private MappedFieldType primaryTermType;
    private MappedFieldType versionType;

    private long rowId;

    @Setup
    public void setup() {
        int n = CLICKBENCH_COLUMN_SHAPE.length();
        fieldTypes = new MappedFieldType[n];
        values = new Object[n];
        isKeyword = new boolean[n];
        for (int i = 0; i < n; i++) {
            String name = "c" + i;
            switch (CLICKBENCH_COLUMN_SHAPE.charAt(i)) {
                case 'K' -> {
                    // Mapping-built keyword types are multi_value-capable and default to AUTO; the
                    // convenience constructor leaves the flag unset, so mirror the builder path here.
                    MappedFieldType keyword = new KeywordFieldMapper.KeywordFieldType(name);
                    keyword.setMultiValueSupported(true);
                    fieldTypes[i] = keyword;
                    values[i] = "keyword-value-" + i;
                    isKeyword[i] = true;
                }
                case 'S' -> {
                    fieldTypes[i] = new NumberFieldMapper.NumberFieldType(name, NumberFieldMapper.NumberType.SHORT);
                    values[i] = (short) i;
                }
                case 'I' -> {
                    fieldTypes[i] = new NumberFieldMapper.NumberFieldType(name, NumberFieldMapper.NumberType.INTEGER);
                    values[i] = i;
                }
                case 'L' -> {
                    fieldTypes[i] = new NumberFieldMapper.NumberFieldType(name, NumberFieldMapper.NumberType.LONG);
                    values[i] = (long) i;
                }
                case 'D' -> {
                    fieldTypes[i] = new DateFieldMapper.DateFieldType(name);
                    values[i] = 1_700_000_000_000L + i;
                }
                default -> throw new IllegalStateException("unknown column shape " + CLICKBENCH_COLUMN_SHAPE.charAt(i));
            }
            assignCapabilities(fieldTypes[i]);
        }
        // Same construction the parquet unit tests use for the engine-populated metadata fields.
        idType = new KeywordFieldMapper.KeywordFieldType(IdFieldMapper.NAME);
        seqNoType = new NumberFieldMapper.NumberFieldType(SeqNoFieldMapper.NAME, NumberFieldMapper.NumberType.LONG);
        primaryTermType = new NumberFieldMapper.NumberFieldType(SeqNoFieldMapper.PRIMARY_TERM_NAME, NumberFieldMapper.NumberType.LONG);
        versionType = new NumberFieldMapper.NumberFieldType(VersionFieldMapper.NAME, NumberFieldMapper.NumberType.LONG);
        assignCapabilities(idType);
        assignCapabilities(seqNoType);
        assignCapabilities(primaryTermType);
        assignCapabilities(versionType);
        rowId = 0;
    }

    /** Mirrors what {@code FieldCapabilityAssigner.assign} does at mapping build time. */
    private static void assignCapabilities(MappedFieldType fieldType) {
        PARQUET.supportedFields()
            .stream()
            .filter(ftc -> ftc.fieldType().equals(fieldType.typeName()))
            .findFirst()
            .ifPresent(ftc -> fieldType.setCapabilityMap(Map.of(PARQUET, ftc.capabilities())));
    }

    /**
     * One full document: a fresh input (the engine calls {@code newDocumentInput()} per document),
     * 106 source fields, 4 metadata fields, then finalization.
     */
    @Benchmark
    public void collectClickBenchDocument(Blackhole bh) {
        ParquetDocumentInput input = new ParquetDocumentInput();
        for (int i = 0; i < fieldTypes.length; i++) {
            MappedFieldType ft = fieldTypes[i];
            if (isKeyword[i] && preCheck) {
                // ParametrizedFieldMapper#addFieldForPluggableFormat, AUTO state, scalar so far.
                if (ft.isMultiValued() == false && ft.isMultiValueSupported() && input.getFieldCount(ft.name()) > 0) {
                    throw new IllegalStateException("unexpected duplicate on scalar workload");
                }
            }
            input.addField(ft, values[i]);
        }
        input.addField(seqNoType, rowId);
        input.addField(idType, "id-" + rowId);
        input.addField(versionType, 1L);
        input.addField(primaryTermType, 1L);
        input.setRowId(DocumentInput.ROW_ID_FIELD, rowId++);
        for (FieldValuePair pair : input.getFinalInput()) {
            bh.consume(pair);
        }
    }
}
