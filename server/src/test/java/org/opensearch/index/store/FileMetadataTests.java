/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.opensearch.test.OpenSearchTestCase;

public class FileMetadataTests extends OpenSearchTestCase {

    // ═══════════════════════════════════════════════════════════════
    // Two-arg constructor tests
    // ═══════════════════════════════════════════════════════════════

    public void testConstructorTwoArgs() {
        FileMetadata fm = new FileMetadata("parquet", "_0_1.parquet");
        assertEquals("parquet", fm.dataFormat());
        assertEquals("_0_1.parquet", fm.file());
    }

    public void testConstructorTwoArgs_lucene() {
        FileMetadata fm = new FileMetadata("lucene", "_0.si");
        assertEquals("lucene", fm.dataFormat());
        assertEquals("_0.si", fm.file());
    }

    // ═══════════════════════════════════════════════════════════════
    // Single-arg constructor tests
    // ═══════════════════════════════════════════════════════════════

    public void testConstructorSingleArg_luceneFileWithoutDelimiter() {
        // A plain filename without delimiter defaults to "lucene"
        FileMetadata fm = new FileMetadata("_0.si");
        assertEquals("lucene", fm.dataFormat());
        assertEquals("_0.si", fm.file());
    }

    public void testConstructorSingleArg_withDelimiter() {
        FileMetadata fm = new FileMetadata("parquet/_0_1.parquet");
        assertEquals("parquet", fm.dataFormat());
        assertEquals("_0_1.parquet", fm.file());
    }

    public void testConstructorSingleArg_withDelimiterLucene() {
        // Plain lucene files don't have a prefix, so single-arg defaults to "lucene"
        FileMetadata fm = new FileMetadata("_0.si");
        assertEquals("lucene", fm.dataFormat());
        assertEquals("_0.si", fm.file());
    }

    public void testConstructorSingleArg_metadataKey() {
        // Files starting with "metadata" and not containing delimiter are treated as metadata format
        FileMetadata fm = new FileMetadata("metadata__1__2__3");
        assertEquals("metadata", fm.dataFormat());
        assertEquals("metadata__1__2__3", fm.file());
    }

    public void testConstructorSingleArg_metadataKeyWithDelimiter() {
        // If it contains delimiter, parse normally even if it starts with "metadata"
        FileMetadata fm = new FileMetadata("metadata/metadata__1__2__3");
        assertEquals("metadata", fm.dataFormat());
        assertEquals("metadata__1__2__3", fm.file());
    }

    // ═══════════════════════════════════════════════════════════════
    // serialize / toString
    // ═══════════════════════════════════════════════════════════════

    public void testSerialize() {
        FileMetadata fm = new FileMetadata("parquet", "_0_1.parquet");
        assertEquals("parquet/_0_1.parquet", fm.serialize());
    }

    public void testSerialize_lucene() {
        FileMetadata fm = new FileMetadata("lucene", "_0.si");
        assertEquals("_0.si", fm.serialize());
    }

    public void testToStringEqualsSerialized() {
        FileMetadata fm = new FileMetadata("arrow", "data.arrow");
        assertEquals(fm.serialize(), fm.toString());
    }

    // ═══════════════════════════════════════════════════════════════
    // Roundtrip: two-arg -> serialize -> single-arg
    // ═══════════════════════════════════════════════════════════════

    public void testRoundtrip() {
        FileMetadata original = new FileMetadata("parquet", "_0_1.parquet");
        String serialized = original.serialize();
        FileMetadata deserialized = new FileMetadata(serialized);
        assertEquals(original, deserialized);
        assertEquals(original.file(), deserialized.file());
        assertEquals(original.dataFormat(), deserialized.dataFormat());
    }

    public void testRoundtrip_lucene() {
        FileMetadata original = new FileMetadata("lucene", "_0.cfs");
        String serialized = original.serialize();
        FileMetadata deserialized = new FileMetadata(serialized);
        assertEquals(original, deserialized);
    }

    // ═══════════════════════════════════════════════════════════════
    // equals / hashCode
    // ═══════════════════════════════════════════════════════════════

    public void testEquals_sameValues() {
        FileMetadata fm1 = new FileMetadata("parquet", "_0.parquet");
        FileMetadata fm2 = new FileMetadata("parquet", "_0.parquet");
        assertEquals(fm1, fm2);
        assertEquals(fm1.hashCode(), fm2.hashCode());
    }

    public void testNotEqual_differentFile() {
        FileMetadata fm1 = new FileMetadata("parquet", "_0.parquet");
        FileMetadata fm2 = new FileMetadata("parquet", "_1.parquet");
        assertNotEquals(fm1, fm2);
    }

    public void testNotEqual_differentFormat() {
        FileMetadata fm1 = new FileMetadata("parquet", "_0.parquet");
        FileMetadata fm2 = new FileMetadata("arrow", "_0.parquet");
        assertNotEquals(fm1, fm2);
    }

    public void testEquals_null() {
        FileMetadata fm = new FileMetadata("lucene", "_0.si");
        assertNotEquals(null, fm);
    }

    public void testEquals_differentType() {
        FileMetadata fm = new FileMetadata("lucene", "_0.si");
        assertNotEquals("_0.si", fm);
    }

    public void testEquals_self() {
        FileMetadata fm = new FileMetadata("lucene", "_0.si");
        assertEquals(fm, fm);
    }

    // ═══════════════════════════════════════════════════════════════
    // DELIMITER constant
    // ═══════════════════════════════════════════════════════════════

    public void testDelimiterConstant() {
        assertEquals("/", FileMetadata.DELIMITER);
    }

    // ═══════════════════════════════════════════════════════════════
    // Edge cases - single-arg constructor
    // ═══════════════════════════════════════════════════════════════

    public void testConstructorSingleArg_segmentsFile() {
        // Segments files (e.g., "segments_1") should default to "lucene" format
        FileMetadata fm = new FileMetadata("segments_1");
        assertEquals("lucene", fm.dataFormat());
        assertEquals("segments_1", fm.file());
    }

    public void testConstructorSingleArg_metadataExactString() {
        // The exact string "metadata" (without underscores/suffixes) should still be treated as metadata
        FileMetadata fm = new FileMetadata("metadata");
        assertEquals("metadata", fm.dataFormat());
        assertEquals("metadata", fm.file());
    }

    public void testConstructorSingleArg_arrowWithDelimiter() {
        FileMetadata fm = new FileMetadata("arrow/data.arrow");
        assertEquals("arrow", fm.dataFormat());
        assertEquals("data.arrow", fm.file());
    }

    public void testConstructorSingleArg_customFormat() {
        FileMetadata fm = new FileMetadata("myformat/data.custom");
        assertEquals("myformat", fm.dataFormat());
        assertEquals("data.custom", fm.file());
    }

    // ═══════════════════════════════════════════════════════════════
    // Roundtrip - metadata format
    // ═══════════════════════════════════════════════════════════════

    public void testRoundtrip_metadata() {
        FileMetadata original = new FileMetadata("metadata", "metadata__1__2__3");
        String serialized = original.serialize();
        assertEquals("metadata/metadata__1__2__3", serialized);
        FileMetadata deserialized = new FileMetadata(serialized);
        assertEquals(original, deserialized);
    }

    public void testRoundtrip_arrow() {
        FileMetadata original = new FileMetadata("arrow", "data.arrow");
        String serialized = original.serialize();
        FileMetadata deserialized = new FileMetadata(serialized);
        assertEquals(original, deserialized);
    }

    // ═══════════════════════════════════════════════════════════════
    // hashCode - inequality
    // ═══════════════════════════════════════════════════════════════

    public void testHashCode_differentObjects_likelyDifferent() {
        FileMetadata fm1 = new FileMetadata("parquet", "_0.parquet");
        FileMetadata fm2 = new FileMetadata("parquet", "_1.parquet");
        // Different content should (very likely) produce different hashCodes
        assertNotEquals(fm1.hashCode(), fm2.hashCode());
    }

    public void testHashCode_differentFormat_likelyDifferent() {
        FileMetadata fm1 = new FileMetadata("parquet", "_0.data");
        FileMetadata fm2 = new FileMetadata("arrow", "_0.data");
        assertNotEquals(fm1.hashCode(), fm2.hashCode());
    }

    // ═══════════════════════════════════════════════════════════════
    // Use as Map key
    // ═══════════════════════════════════════════════════════════════

    public void testUsableAsMapKey() {
        java.util.Map<FileMetadata, String> map = new java.util.HashMap<>();
        FileMetadata key1 = new FileMetadata("parquet", "_0.parquet");
        FileMetadata key2 = new FileMetadata("parquet", "_0.parquet"); // equal to key1

        map.put(key1, "value1");
        assertEquals("value1", map.get(key2)); // should find by equal key
        assertEquals(1, map.size());
    }

    public void testUsableAsMapKey_differentFormats() {
        java.util.Map<FileMetadata, String> map = new java.util.HashMap<>();
        FileMetadata key1 = new FileMetadata("parquet", "_0.data");
        FileMetadata key2 = new FileMetadata("arrow", "_0.data");

        map.put(key1, "parquet-value");
        map.put(key2, "arrow-value");
        assertEquals(2, map.size());
        assertEquals("parquet-value", map.get(key1));
        assertEquals("arrow-value", map.get(key2));
    }

    // ═══════════════════════════════════════════════════════════════
    // Constructor consistency: two-arg vs single-arg from serialized
    // ═══════════════════════════════════════════════════════════════

    public void testTwoArgAndSingleArgConsistency() {
        FileMetadata twoArg = new FileMetadata("parquet", "_0.parquet");
        FileMetadata singleArg = new FileMetadata(twoArg.serialize());
        assertEquals(twoArg, singleArg);
        assertEquals(twoArg.file(), singleArg.file());
        assertEquals(twoArg.dataFormat(), singleArg.dataFormat());
        assertEquals(twoArg.hashCode(), singleArg.hashCode());
    }

    public void testFileAndDataFormatImmutable() {
        FileMetadata fm = new FileMetadata("lucene", "_0.si");
        // Verify accessors return consistent values on repeated calls
        assertEquals("lucene", fm.dataFormat());
        assertEquals("lucene", fm.dataFormat());
        assertEquals("_0.si", fm.file());
        assertEquals("_0.si", fm.file());
    }
}
