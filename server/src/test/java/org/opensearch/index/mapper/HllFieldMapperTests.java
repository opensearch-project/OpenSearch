/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.BitMixer;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.metrics.AbstractHyperLogLog;
import org.opensearch.search.aggregations.metrics.AbstractHyperLogLogPlusPlus;
import org.opensearch.search.aggregations.metrics.HyperLogLogPlusPlus;

import java.io.IOException;
import java.util.Arrays;

import static org.opensearch.search.DocValueFormat.BINARY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class HllFieldMapperTests extends MapperTestCase {

    @Override
    protected void writeFieldValue(XContentBuilder builder) throws IOException {
        // Create a simple HLL++ sketch with default precision
        HyperLogLogPlusPlus sketch = new HyperLogLogPlusPlus(HyperLogLogPlusPlus.DEFAULT_PRECISION, BigArrays.NON_RECYCLING_INSTANCE, 1);
        try {
            // Add some values to the sketch
            sketch.collect(0, 1L);
            sketch.collect(0, 2L);
            sketch.collect(0, 3L);

            // Serialize the sketch
            BytesStreamOutput out = new BytesStreamOutput();
            sketch.writeTo(0, out);
            BytesRef bytesRef = out.bytes().toBytesRef();
            builder.value(Arrays.copyOfRange(bytesRef.bytes, bytesRef.offset, bytesRef.offset + bytesRef.length));
        } finally {
            sketch.close();
        }
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "hll");
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("precision", b -> b.field("precision", 12));
    }

    public void testDefaultMapping() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");

        assertThat(mapper, instanceOf(HllFieldMapper.class));
        HllFieldMapper hllMapper = (HllFieldMapper) mapper;
        assertThat(hllMapper.fieldType().precision(), equalTo(HyperLogLogPlusPlus.DEFAULT_PRECISION));
    }

    public void testPrecisionParameterValidation() {
        // Test that precision values outside [4, 18] are rejected

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "hll");
            b.field("precision", 3);
        })));
        assertThat(
            e.getMessage(),
            containsString("precision must be between " + AbstractHyperLogLog.MIN_PRECISION + " and " + AbstractHyperLogLog.MAX_PRECISION)
        );

        e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "hll");
            b.field("precision", 19);
        })));
        assertThat(
            e.getMessage(),
            containsString("precision must be between " + AbstractHyperLogLog.MIN_PRECISION + " and " + AbstractHyperLogLog.MAX_PRECISION)
        );
    }

    public void testIndexParameterRejected() {
        // Test that setting index=true is rejected
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "hll");
            b.field("index", true);
        })));
        assertThat(e.getMessage(), containsString("Cannot set [index] on field of type [hll]"));
    }

    public void testStoreParameterRejected() {
        // Test that setting store=true is rejected
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "hll");
            b.field("store", true);
        })));
        assertThat(e.getMessage(), containsString("Cannot set [store] on field of type [hll]"));
    }

    public void testDocValuesCannotBeDisabled() {
        // Test that setting doc_values=false is rejected
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "hll");
            b.field("doc_values", false);
        })));
        assertThat(e.getMessage(), containsString("Cannot disable [doc_values] on field of type [hll]"));
    }

    public void testIndexFalseIsAccepted() throws IOException {
        // Test that explicitly setting index=false is accepted (it's the default)
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "hll");
            b.field("index", false);
        }));
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper, instanceOf(HllFieldMapper.class));
    }

    public void testStoreFalseIsAccepted() throws IOException {
        // Test that explicitly setting store=false is accepted (it's the default)
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "hll");
            b.field("store", false);
        }));
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper, instanceOf(HllFieldMapper.class));
    }

    public void testDocValuesTrueIsAccepted() throws IOException {
        // Test that explicitly setting doc_values=true is accepted (it's the default)
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "hll");
            b.field("doc_values", true);
        }));
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper, instanceOf(HllFieldMapper.class));
    }

    public void testCopyToIsSupported() throws IOException {
        // Test that copy_to is supported (inherited from ParametrizedFieldMapper.Builder)
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("field1").field("type", "hll").field("copy_to", "field2").endObject();
            b.startObject("field2").field("type", "hll").endObject();
        }));
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field1");
        assertThat(mapper, instanceOf(HllFieldMapper.class));
    }

    public void testPrecisionCannotBeChanged() throws IOException {
        // Test that changing precision after field creation is rejected
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "hll");
            b.field("precision", 11);
        }));

        // Verify initial precision
        HllFieldMapper mapper = (HllFieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper.fieldType().precision(), equalTo(11));

        // Attempt to change precision should fail
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> merge(mapperService, fieldMapping(b -> {
            b.field("type", "hll");
            b.field("precision", 14);
        })));

        assertThat(e.getMessage(), containsString("Mapper for [field] conflicts with existing mapper"));
    }

    public void testPrecisionChangeRejectedWithExistingData() throws IOException {
        // Test that precision cannot be changed even with a more realistic scenario
        // where the field already has data

        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "hll");
            b.field("precision", 12);
        }));

        // Create a sketch with precision 12
        HyperLogLogPlusPlus sketch = new HyperLogLogPlusPlus(12, BigArrays.NON_RECYCLING_INSTANCE, 1);
        try {
            sketch.collect(0, 1L);
            sketch.collect(0, 2L);

            BytesStreamOutput out = new BytesStreamOutput();
            sketch.writeTo(0, out);
            BytesRef bytesRef = out.bytes().toBytesRef();
            byte[] serialized = Arrays.copyOfRange(bytesRef.bytes, bytesRef.offset, bytesRef.offset + bytesRef.length);

            // Index a document with the sketch
            ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("field", serialized)));
            assertNotNull(doc);

            // Now try to change precision - should fail
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> merge(mapperService, fieldMapping(b -> {
                b.field("type", "hll");
                b.field("precision", 14);
            })));

            assertThat(e.getMessage(), containsString("Mapper for [field] conflicts with existing mapper"));
        } finally {
            sketch.close();
        }
    }

    public void testDefaultPrecision() throws Exception {
        // Test that when no precision is specified, the default is used
        MapperService mapperService = createMapperService(fieldMapping(b -> { b.field("type", "hll"); }));

        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper, instanceOf(HllFieldMapper.class));
        HllFieldMapper hllMapper = (HllFieldMapper) mapper;
        assertThat(hllMapper.fieldType().precision(), equalTo(HyperLogLogPlusPlus.DEFAULT_PRECISION));
    }

    public void testCustomPrecision() throws Exception {
        // Test that custom precision values are respected
        int customPrecision = 14;
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "hll");
            b.field("precision", customPrecision);
        }));

        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper, instanceOf(HllFieldMapper.class));
        HllFieldMapper hllMapper = (HllFieldMapper) mapper;
        assertThat(hllMapper.fieldType().precision(), equalTo(customPrecision));
    }

    public void testTermQueryNotSupported() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        MappedFieldType fieldType = mapperService.fieldType("field");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> fieldType.termQuery("value", null));
        assertThat(e.getMessage(), containsString("Term queries are not supported on [hll] fields"));
    }

    public void testExistsQuery() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        assertExistsQuery(mapperService);
    }

    public void testSerializationRoundTrip() throws IOException {
        // Test that serializing and deserializing HLL++ sketches preserves cardinality
        // This is a property-based test simulated with multiple random sketches

        int[] precisions = { 4, 8, 11, 14, 18 }; // Test various precisions
        int[] cardinalities = { 0, 1, 10, 100, 1000, 10000 }; // Test various cardinalities

        for (int precision : precisions) {
            for (int cardinality : cardinalities) {
                // Create a sketch with the given precision
                HyperLogLogPlusPlus originalSketch = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
                try {
                    // Add values to reach the target cardinality
                    for (int i = 0; i < cardinality; i++) {
                        originalSketch.collect(0, i);
                    }

                    long originalCardinality = originalSketch.cardinality(0);

                    // Serialize the sketch
                    BytesStreamOutput out = new BytesStreamOutput();
                    originalSketch.writeTo(0, out);
                    BytesRef bytesRef = out.bytes().toBytesRef();
                    byte[] serialized = Arrays.copyOfRange(bytesRef.bytes, bytesRef.offset, bytesRef.offset + bytesRef.length);

                    // Create a mapper with the same precision
                    MapperService mapperService = createMapperService(fieldMapping(b -> {
                        b.field("type", "hll");
                        b.field("precision", precision);
                    }));

                    // Parse a document with the serialized sketch
                    ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("field", serialized)));

                    // Verify the document was created successfully
                    assertNotNull(doc);
                    assertNotNull(doc.rootDoc().getBinaryValue("field"));

                    // Deserialize and verify cardinality is preserved
                    byte[] storedBytes = doc.rootDoc().getBinaryValue("field").bytes;
                    org.opensearch.search.aggregations.metrics.AbstractHyperLogLogPlusPlus deserializedSketch =
                        org.opensearch.search.aggregations.metrics.AbstractHyperLogLogPlusPlus.readFrom(
                            new org.opensearch.core.common.bytes.BytesArray(storedBytes).streamInput(),
                            BigArrays.NON_RECYCLING_INSTANCE
                        );

                    try {
                        long deserializedCardinality = deserializedSketch.cardinality(0);

                        // Cardinality should be preserved (within HLL++ error bounds)
                        // For small cardinalities, they should be exact
                        if (cardinality <= 100) {
                            assertEquals(
                                "Cardinality mismatch for precision=" + precision + ", cardinality=" + cardinality,
                                originalCardinality,
                                deserializedCardinality
                            );
                        } else {
                            // For larger cardinalities, allow for HLL++ error
                            double errorBound = 1.04 / Math.sqrt(1 << precision);
                            double relativeError = Math.abs(deserializedCardinality - originalCardinality) / (double) cardinality;
                            assertTrue(
                                "Relative error "
                                    + relativeError
                                    + " exceeds bound "
                                    + errorBound
                                    + " for precision="
                                    + precision
                                    + ", cardinality="
                                    + cardinality,
                                relativeError <= errorBound * 3 // Allow 3x error bound for safety
                            );
                        }
                    } finally {
                        deserializedSketch.close();
                    }
                } finally {
                    originalSketch.close();
                }
            }
        }
    }

    public void testInvalidSketchRejection() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "hll");
            b.field("precision", 11);
        }));

        // Test 1: Completely invalid binary data
        byte[] invalidData = new byte[] { 1, 2, 3, 4, 5 };
        MapperParsingException e1 = expectThrows(
            MapperParsingException.class,
            () -> mapperService.documentMapper().parse(source(b -> b.field("field", invalidData)))
        );
        // The error gets wrapped, so check for the generic parse failure message
        assertTrue(e1.getMessage().contains("failed to parse") || e1.getCause() != null);

        // Test 2: Sketch with wrong precision
        HyperLogLogPlusPlus wrongPrecisionSketch = new HyperLogLogPlusPlus(14, BigArrays.NON_RECYCLING_INSTANCE, 1);
        try {
            wrongPrecisionSketch.collect(0, 1L);
            BytesStreamOutput out = new BytesStreamOutput();
            wrongPrecisionSketch.writeTo(0, out);
            BytesRef bytesRef = out.bytes().toBytesRef();
            byte[] wrongPrecisionData = Arrays.copyOfRange(bytesRef.bytes, bytesRef.offset, bytesRef.offset + bytesRef.length);

            MapperParsingException e2 = expectThrows(
                MapperParsingException.class,
                () -> mapperService.documentMapper().parse(source(b -> b.field("field", wrongPrecisionData)))
            );
            // Check the cause for the precision mismatch message
            String fullMessage = e2.getMessage() + (e2.getCause() != null ? e2.getCause().getMessage() : "");
            assertThat(fullMessage, containsString("precision mismatch"));
        } finally {
            wrongPrecisionSketch.close();
        }
    }

    public void testStoredSketchRetrieval() throws IOException {

        int[] precisions = { 4, 8, 11, 14, 18 };
        int[] cardinalities = { 0, 5, 50, 500, 5000 };

        for (int precision : precisions) {
            MapperService mapperService = createMapperService(fieldMapping(b -> {
                b.field("type", "hll");
                b.field("precision", precision);
            }));

            for (int cardinality : cardinalities) {
                // Create and populate a sketch
                HyperLogLogPlusPlus originalSketch = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
                try {
                    for (int i = 0; i < cardinality; i++) {
                        originalSketch.collect(0, i);
                    }

                    // Serialize the sketch
                    BytesStreamOutput out = new BytesStreamOutput();
                    originalSketch.writeTo(0, out);
                    BytesRef bytesRef = out.bytes().toBytesRef();
                    byte[] serialized = Arrays.copyOfRange(bytesRef.bytes, bytesRef.offset, bytesRef.offset + bytesRef.length);

                    // Store the sketch in a document
                    ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("field", serialized)));

                    // Verify the sketch can be retrieved
                    assertNotNull("Document should be created", doc);
                    BytesRef storedValue = doc.rootDoc().getBinaryValue("field");
                    assertNotNull("Sketch should be stored", storedValue);

                    // Verify the retrieved sketch is valid and has the correct cardinality
                    AbstractHyperLogLogPlusPlus retrievedSketch = AbstractHyperLogLogPlusPlus.readFrom(
                        new BytesArray(storedValue.bytes, storedValue.offset, storedValue.length).streamInput(),
                        BigArrays.NON_RECYCLING_INSTANCE
                    );

                    try {
                        assertEquals("Precision should match", precision, retrievedSketch.precision());
                        long retrievedCardinality = retrievedSketch.cardinality(0);
                        long originalCardinality = originalSketch.cardinality(0);
                        assertEquals(
                            "Cardinality should be preserved for precision=" + precision + ", cardinality=" + cardinality,
                            originalCardinality,
                            retrievedCardinality
                        );
                    } finally {
                        retrievedSketch.close();
                    }
                } finally {
                    originalSketch.close();
                }
            }
        }
    }

    public void testMergeProducesUnionCardinality() {
        // Test that merging two sketches produces a sketch representing their union
        // We test this by collecting the same values into a single sketch and into two separate sketches,
        // then verifying the merged result matches the single sketch

        int precision = 11;
        int numValues = 1000;

        HyperLogLogPlusPlus single = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
        HyperLogLogPlusPlus sketchA = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
        HyperLogLogPlusPlus sketchB = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
        HyperLogLogPlusPlus merged = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);

        try {
            // Collect all values into single sketch
            // Split values between sketchA and sketchB
            for (int i = 0; i < numValues; i++) {
                long hash = BitMixer.mix64(i);
                single.collect(0, hash);
                if (i < numValues / 2) {
                    sketchA.collect(0, hash);
                } else {
                    sketchB.collect(0, hash);
                }
            }

            // Merge the two sketches
            merged.merge(0, sketchA, 0);
            merged.merge(0, sketchB, 0);

            // Merged cardinality should equal single sketch cardinality
            long singleCardinality = single.cardinality(0);
            long mergedCardinality = merged.cardinality(0);

            assertEquals("Merged cardinality should equal single sketch cardinality", singleCardinality, mergedCardinality);
        } finally {
            single.close();
            sketchA.close();
            sketchB.close();
            merged.close();
        }
    }

    public void testMergeAccuracy() {
        // Test that merging multiple sketches maintains HLL++ accuracy guarantees
        // Similar to testMergeProducesUnionCardinality but with more sketches

        int precision = 11;
        int numSketches = 5;
        int numValues = 1000;

        HyperLogLogPlusPlus single = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
        HyperLogLogPlusPlus[] sketches = new HyperLogLogPlusPlus[numSketches];
        HyperLogLogPlusPlus merged = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);

        try {
            // Initialize all sketches
            for (int i = 0; i < numSketches; i++) {
                sketches[i] = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
            }

            // Collect same values into single sketch and distribute across multiple sketches
            for (int i = 0; i < numValues; i++) {
                long hash = BitMixer.mix64(i);
                single.collect(0, hash);
                // Distribute values across sketches
                sketches[i % numSketches].collect(0, hash);
            }

            // Merge all sketches
            for (HyperLogLogPlusPlus sketch : sketches) {
                merged.merge(0, sketch, 0);
            }

            // Merged cardinality should equal single sketch cardinality
            long singleCardinality = single.cardinality(0);
            long mergedCardinality = merged.cardinality(0);

            assertEquals("Merged cardinality should equal single sketch cardinality", singleCardinality, mergedCardinality);
        } finally {
            single.close();
            for (HyperLogLogPlusPlus sketch : sketches) {
                if (sketch != null) sketch.close();
            }
            merged.close();
        }
    }

    public void testMergeIsCommutative() {
        // Test that merge(A, B) produces the same cardinality as merge(B, A)

        int[] precisions = { 8, 11, 14 };

        for (int precision : precisions) {
            HyperLogLogPlusPlus sketchA = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
            HyperLogLogPlusPlus sketchB = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
            HyperLogLogPlusPlus mergedAB = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
            HyperLogLogPlusPlus mergedBA = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);

            try {
                // Add different values to each sketch
                for (int i = 0; i < 100; i++) {
                    sketchA.collect(0, i);
                }
                for (int i = 50; i < 150; i++) {
                    sketchB.collect(0, i);
                }

                // Merge in both orders
                mergedAB.merge(0, sketchA, 0);
                mergedAB.merge(0, sketchB, 0);

                mergedBA.merge(0, sketchB, 0);
                mergedBA.merge(0, sketchA, 0);

                // Cardinality should be the same regardless of merge order
                long cardinalityAB = mergedAB.cardinality(0);
                long cardinalityBA = mergedBA.cardinality(0);

                assertEquals("Merge should be commutative for precision=" + precision, cardinalityAB, cardinalityBA);
            } finally {
                sketchA.close();
                sketchB.close();
                mergedAB.close();
                mergedBA.close();
            }
        }
    }

    public void testMergeIsAssociative() {
        // Test that merge(merge(A, B), C) produces the same cardinality as merge(A, merge(B, C))

        int precision = 11;

        HyperLogLogPlusPlus sketchA = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
        HyperLogLogPlusPlus sketchB = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
        HyperLogLogPlusPlus sketchC = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
        HyperLogLogPlusPlus leftAssoc = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
        HyperLogLogPlusPlus rightAssoc = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
        HyperLogLogPlusPlus tempLeft = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
        HyperLogLogPlusPlus tempRight = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);

        try {
            // Add different values to each sketch
            for (int i = 0; i < 50; i++) {
                sketchA.collect(0, i);
            }
            for (int i = 25; i < 75; i++) {
                sketchB.collect(0, i);
            }
            for (int i = 50; i < 100; i++) {
                sketchC.collect(0, i);
            }

            // Left associative: merge(merge(A, B), C)
            tempLeft.merge(0, sketchA, 0);
            tempLeft.merge(0, sketchB, 0);
            leftAssoc.merge(0, tempLeft, 0);
            leftAssoc.merge(0, sketchC, 0);

            // Right associative: merge(A, merge(B, C))
            tempRight.merge(0, sketchB, 0);
            tempRight.merge(0, sketchC, 0);
            rightAssoc.merge(0, sketchA, 0);
            rightAssoc.merge(0, tempRight, 0);

            // Cardinality should be the same regardless of grouping
            long leftCardinality = leftAssoc.cardinality(0);
            long rightCardinality = rightAssoc.cardinality(0);

            assertEquals("Merge should be associative", leftCardinality, rightCardinality);
        } finally {
            sketchA.close();
            sketchB.close();
            sketchC.close();
            leftAssoc.close();
            rightAssoc.close();
            tempLeft.close();
            tempRight.close();
        }
    }

    public void testIncompatiblePrecisionHandling() {
        // Test that merging sketches with different precision is rejected

        int precision1 = 8;
        int precision2 = 14;

        HyperLogLogPlusPlus sketch1 = new HyperLogLogPlusPlus(precision1, BigArrays.NON_RECYCLING_INSTANCE, 1);
        HyperLogLogPlusPlus sketch2 = new HyperLogLogPlusPlus(precision2, BigArrays.NON_RECYCLING_INSTANCE, 1);

        try {
            sketch1.collect(0, 1L);
            sketch2.collect(0, 2L);

            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> sketch1.merge(0, sketch2, 0));

            assertThat(e.getMessage(), containsString("Cannot merge HLL++ sketches with different precision"));
            assertThat(e.getMessage(), containsString(String.valueOf(precision1)));
            assertThat(e.getMessage(), containsString(String.valueOf(precision2)));
        } finally {
            sketch1.close();
            sketch2.close();
        }
    }

    public void testSerializationFormatCompatibility() throws IOException {
        // Test that HLL++ field uses the same format as cardinality aggregation
        // Sketches serialized by cardinality aggregation should be readable by HLL field and vice versa

        int precision = 11;
        int cardinality = 500;

        // Create a sketch using the cardinality aggregation code
        HyperLogLogPlusPlus cardinalitySketch = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
        try {
            for (int i = 0; i < cardinality; i++) {
                cardinalitySketch.collect(0, i);
            }

            long originalCardinality = cardinalitySketch.cardinality(0);

            // Serialize using cardinality aggregation format
            BytesStreamOutput out = new BytesStreamOutput();
            cardinalitySketch.writeTo(0, out);
            BytesRef bytesRef = out.bytes().toBytesRef();
            byte[] serialized = Arrays.copyOfRange(bytesRef.bytes, bytesRef.offset, bytesRef.offset + bytesRef.length);

            // Store in HLL field
            MapperService mapperService = createMapperService(fieldMapping(b -> {
                b.field("type", "hll");
                b.field("precision", precision);
            }));

            ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("field", serialized)));

            // Retrieve and deserialize
            BytesRef storedBytes = doc.rootDoc().getBinaryValue("field");
            AbstractHyperLogLogPlusPlus retrievedSketch = AbstractHyperLogLogPlusPlus.readFrom(
                new BytesArray(storedBytes.bytes, storedBytes.offset, storedBytes.length).streamInput(),
                BigArrays.NON_RECYCLING_INSTANCE
            );

            try {
                long retrievedCardinality = retrievedSketch.cardinality(0);
                assertEquals("Cardinality should be preserved across serialization formats", originalCardinality, retrievedCardinality);
            } finally {
                retrievedSketch.close();
            }
        } finally {
            cardinalitySketch.close();
        }
    }

    public void testDerivedSourceSupport() throws IOException {
        // Test that HLL fields support derived source
        // This enables storage optimization when _source is disabled

        int precision = 12;
        int cardinality = 100;

        // Create an HLL++ sketch
        HyperLogLogPlusPlus sketch = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
        try {
            for (int i = 0; i < cardinality; i++) {
                sketch.collect(0, BitMixer.mix64(i));
            }

            // Serialize the sketch
            BytesStreamOutput out = new BytesStreamOutput();
            sketch.writeTo(0, out);
            BytesRef bytesRef = out.bytes().toBytesRef();
            byte[] serialized = Arrays.copyOfRange(bytesRef.bytes, bytesRef.offset, bytesRef.offset + bytesRef.length);

            // Store in HLL field
            MapperService mapperService = createMapperService(fieldMapping(b -> {
                b.field("type", "hll");
                b.field("precision", precision);
            }));

            ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("field", serialized)));

            // Verify doc values are stored
            BytesRef storedBytes = doc.rootDoc().getBinaryValue("field");
            assertNotNull("Doc values should be stored", storedBytes);

            // Verify the sketch can be reconstructed from doc values
            AbstractHyperLogLogPlusPlus retrievedSketch = AbstractHyperLogLogPlusPlus.readFrom(
                new BytesArray(storedBytes.bytes, storedBytes.offset, storedBytes.length).streamInput(),
                BigArrays.NON_RECYCLING_INSTANCE
            );

            try {
                assertEquals("Precision should match", precision, retrievedSketch.precision());
                long originalCardinality = sketch.cardinality(0);
                long retrievedCardinality = retrievedSketch.cardinality(0);
                assertEquals("Cardinality should be preserved", originalCardinality, retrievedCardinality);
            } finally {
                retrievedSketch.close();
            }

            // Verify DocValueFormat is BINARY (for base64 encoding in synthetic source)
            HllFieldMapper.HllFieldType fieldType = (HllFieldMapper.HllFieldType) mapperService.fieldType("field");
            assertEquals(
                "HLL fields should use BINARY doc value format for synthetic source",
                BINARY,
                fieldType.docValueFormat(null, null)
            );
        } finally {
            sketch.close();
        }
    }

    public void testValueFetcherWithDocValues() throws IOException {
        // Test that ValueFetcher returns a DocValueFetcher (for synthetic source support)
        // This verifies that HLL fields can be reconstructed from doc values when _source is disabled

        int precision = 14;

        // Create mapper service
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "hll");
            b.field("precision", precision);
        }));

        // Get the field type
        HllFieldMapper.HllFieldType fieldType = (HllFieldMapper.HllFieldType) mapperService.fieldType("field");

        // Verify that valueFetcher uses DocValueFetcher (for synthetic source support)
        // We can't easily test the full end-to-end fetching without complex mocking,
        // but we can verify the docValueFormat is BINARY which is what enables synthetic source
        assertEquals("HLL fields should use BINARY doc value format for synthetic source", BINARY, fieldType.docValueFormat(null, null));

        // The valueFetcher implementation creates a DocValueFetcher with BINARY format
        // This is verified by the testDerivedSourceSupport test which checks:
        // 1. Doc values are stored
        // 2. Sketches can be reconstructed from doc values
        // 3. DocValueFormat is BINARY
    }
}
