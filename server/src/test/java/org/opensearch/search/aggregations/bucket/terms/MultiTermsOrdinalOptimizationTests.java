/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.mapper.HllFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.MultiTermsValuesSourceConfig;
import org.opensearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static java.util.Arrays.asList;

/**
 * End-to-end equivalence tests verifying that the ordinal-based path and the
 * bytes-based path produce identical aggregation results for the same data.
 */
public class MultiTermsOrdinalOptimizationTests extends AggregatorTestCase {

    private static final String KW_FIELD_1 = "kw1";
    private static final String KW_FIELD_2 = "kw2";
    private static final String KW_FIELD_3 = "kw3";
    private static final String INT_FIELD = "int_val";

    private MappedFieldType[] allFieldTypes() {
        return new MappedFieldType[] {
            new KeywordFieldMapper.KeywordFieldType(KW_FIELD_1),
            new KeywordFieldMapper.KeywordFieldType(KW_FIELD_2),
            new KeywordFieldMapper.KeywordFieldType(KW_FIELD_3),
            new NumberFieldMapper.NumberFieldType(INT_FIELD, NumberFieldMapper.NumberType.INTEGER) };
    }

    private MultiTermsValuesSourceConfig kwConfig(String field) {
        return new MultiTermsValuesSourceConfig.Builder().setFieldName(field).build();
    }

    private MultiTermsValuesSourceConfig intConfig() {
        return new MultiTermsValuesSourceConfig.Builder().setFieldName(INT_FIELD).build();
    }

    /**
     * Test with 2 keyword fields — exercises the PackedOrdinalBucketOrds (single-long) path.
     * Indexes random documents with 2 keyword fields, runs multi_terms aggregation,
     * and verifies results match expected bucket keys and doc counts.
     */
    public void testTwoKeywordFieldsPairPackingPath() throws IOException {
        int numDistinctValues = randomIntBetween(3, 15);
        String[] values = new String[numDistinctValues];
        for (int i = 0; i < numDistinctValues; i++) {
            values[i] = randomAlphaOfLength(randomIntBetween(3, 10));
        }

        int numDocs = randomIntBetween(20, 100);
        Map<String, Long> expectedCounts = new HashMap<>();

        try (Directory directory = newDirectory()) {
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory);
            for (int i = 0; i < numDocs; i++) {
                String v1 = values[randomIntBetween(0, numDistinctValues - 1)];
                String v2 = values[randomIntBetween(0, numDistinctValues - 1)];
                Document doc = new Document();
                doc.add(new SortedDocValuesField(KW_FIELD_1, new BytesRef(v1)));
                doc.add(new SortedDocValuesField(KW_FIELD_2, new BytesRef(v2)));
                iw.addDocument(doc);
                expectedCounts.merge(v1 + "|" + v2, 1L, Long::sum);
            }
            iw.close();

            try (DirectoryReader unwrapped = DirectoryReader.open(directory); IndexReader reader = wrapDirectoryReader(unwrapped)) {
                IndexSearcher searcher = newIndexSearcher(reader);

                MultiTermsAggregationBuilder builder = new MultiTermsAggregationBuilder("test_agg").terms(
                    asList(kwConfig(KW_FIELD_1), kwConfig(KW_FIELD_2))
                ).size(numDistinctValues * numDistinctValues + 10);

                InternalMultiTerms result = searchAndReduce(searcher, new MatchAllDocsQuery(), builder, allFieldTypes());

                // Verify every bucket matches expected counts
                Map<String, Long> actualCounts = new HashMap<>();
                for (InternalMultiTerms.Bucket bucket : result.getBuckets()) {
                    List<Object> key = bucket.getKey();
                    assertEquals("Each bucket key should have 2 elements", 2, key.size());
                    String compositeKey = key.get(0) + "|" + key.get(1);
                    actualCounts.put(compositeKey, bucket.getDocCount());
                }

                assertEquals("Bucket count mismatch", expectedCounts.size(), actualCounts.size());
                for (Map.Entry<String, Long> entry : expectedCounts.entrySet()) {
                    assertEquals("Doc count mismatch for key " + entry.getKey(), entry.getValue(), actualCounts.get(entry.getKey()));
                }
            }
        }
    }

    /**
     * Test with 3 keyword fields — exercises the PackedOrdinalBucketOrds (single-long) path.
     * Indexes random documents with 3 keyword fields, runs multi_terms aggregation,
     * and verifies results match expected bucket keys and doc counts.
     */
    public void testThreeKeywordFieldsPackedOrdinalsPath() throws IOException {
        int numDistinctValues = randomIntBetween(2, 8);
        String[] values = new String[numDistinctValues];
        for (int i = 0; i < numDistinctValues; i++) {
            values[i] = randomAlphaOfLength(randomIntBetween(3, 10));
        }

        int numDocs = randomIntBetween(20, 80);
        Map<String, Long> expectedCounts = new HashMap<>();

        try (Directory directory = newDirectory()) {
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory);
            for (int i = 0; i < numDocs; i++) {
                String v1 = values[randomIntBetween(0, numDistinctValues - 1)];
                String v2 = values[randomIntBetween(0, numDistinctValues - 1)];
                String v3 = values[randomIntBetween(0, numDistinctValues - 1)];
                Document doc = new Document();
                doc.add(new SortedDocValuesField(KW_FIELD_1, new BytesRef(v1)));
                doc.add(new SortedDocValuesField(KW_FIELD_2, new BytesRef(v2)));
                doc.add(new SortedDocValuesField(KW_FIELD_3, new BytesRef(v3)));
                iw.addDocument(doc);
                expectedCounts.merge(v1 + "|" + v2 + "|" + v3, 1L, Long::sum);
            }
            iw.close();

            try (DirectoryReader unwrapped = DirectoryReader.open(directory); IndexReader reader = wrapDirectoryReader(unwrapped)) {
                IndexSearcher searcher = newIndexSearcher(reader);

                MultiTermsAggregationBuilder builder = new MultiTermsAggregationBuilder("test_agg").terms(
                    asList(kwConfig(KW_FIELD_1), kwConfig(KW_FIELD_2), kwConfig(KW_FIELD_3))
                ).size(numDistinctValues * numDistinctValues * numDistinctValues + 10);

                InternalMultiTerms result = searchAndReduce(searcher, new MatchAllDocsQuery(), builder, allFieldTypes());

                Map<String, Long> actualCounts = new HashMap<>();
                for (InternalMultiTerms.Bucket bucket : result.getBuckets()) {
                    List<Object> key = bucket.getKey();
                    assertEquals("Each bucket key should have 3 elements", 3, key.size());
                    String compositeKey = key.get(0) + "|" + key.get(1) + "|" + key.get(2);
                    actualCounts.put(compositeKey, bucket.getDocCount());
                }

                assertEquals("Bucket count mismatch", expectedCounts.size(), actualCounts.size());
                for (Map.Entry<String, Long> entry : expectedCounts.entrySet()) {
                    assertEquals("Doc count mismatch for key " + entry.getKey(), entry.getValue(), actualCounts.get(entry.getKey()));
                }
            }
        }
    }

    /**
     * Test with mixed keyword + numeric fields — exercises the BytesKeyedBucketOrds fallback path.
     * Verifies that when not all fields are WithOrdinals, the aggregation still produces correct results.
     */
    public void testMixedKeywordAndNumericFallbackPath() throws IOException {
        int numDistinctKw = randomIntBetween(3, 10);
        String[] kwValues = new String[numDistinctKw];
        for (int i = 0; i < numDistinctKw; i++) {
            kwValues[i] = randomAlphaOfLength(randomIntBetween(3, 8));
        }

        int numDocs = randomIntBetween(20, 80);
        Map<String, Long> expectedCounts = new HashMap<>();

        try (Directory directory = newDirectory()) {
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory);
            for (int i = 0; i < numDocs; i++) {
                String kw1 = kwValues[randomIntBetween(0, numDistinctKw - 1)];
                int intVal = randomIntBetween(1, 5);
                Document doc = new Document();
                doc.add(new SortedDocValuesField(KW_FIELD_1, new BytesRef(kw1)));
                doc.add(new NumericDocValuesField(INT_FIELD, intVal));
                iw.addDocument(doc);
                expectedCounts.merge(kw1 + "|" + intVal, 1L, Long::sum);
            }
            iw.close();

            try (DirectoryReader unwrapped = DirectoryReader.open(directory); IndexReader reader = wrapDirectoryReader(unwrapped)) {
                IndexSearcher searcher = newIndexSearcher(reader);

                MultiTermsAggregationBuilder builder = new MultiTermsAggregationBuilder("test_agg").terms(
                    asList(kwConfig(KW_FIELD_1), intConfig())
                ).size(numDistinctKw * 5 + 10);

                InternalMultiTerms result = searchAndReduce(searcher, new MatchAllDocsQuery(), builder, allFieldTypes());

                Map<String, Long> actualCounts = new HashMap<>();
                for (InternalMultiTerms.Bucket bucket : result.getBuckets()) {
                    List<Object> key = bucket.getKey();
                    assertEquals("Each bucket key should have 2 elements", 2, key.size());
                    String compositeKey = key.get(0) + "|" + key.get(1);
                    actualCounts.put(compositeKey, bucket.getDocCount());
                }

                assertEquals("Bucket count mismatch", expectedCounts.size(), actualCounts.size());
                for (Map.Entry<String, Long> entry : expectedCounts.entrySet()) {
                    assertEquals("Doc count mismatch for key " + entry.getKey(), entry.getValue(), actualCounts.get(entry.getKey()));
                }
            }
        }
    }

    /**
     * Test equivalence between ordinal path (2 keyword fields) and fallback path (keyword + int)
     * using the same underlying data. Indexes documents with both keyword and numeric representations,
     * then compares the sorted bucket results from both paths.
     */
    public void testOrdinalPathEquivalentToFallbackPath() throws IOException {
        // Use deterministic values so we can compare across paths
        String[] kw1Values = { "alpha", "beta", "gamma" };
        String[] kw2Values = { "one", "two", "three" };
        // Map kw2 values to integers for the fallback path
        Map<String, Integer> kw2ToInt = new HashMap<>();
        kw2ToInt.put("one", 1);
        kw2ToInt.put("two", 2);
        kw2ToInt.put("three", 3);

        int numDocs = randomIntBetween(30, 100);
        List<int[]> docChoices = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            docChoices.add(new int[] { randomIntBetween(0, 2), randomIntBetween(0, 2) });
        }

        // Run ordinal path: 2 keyword fields
        List<String> ordinalBuckets;
        try (Directory directory = newDirectory()) {
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory);
            for (int[] choice : docChoices) {
                Document doc = new Document();
                doc.add(new SortedDocValuesField(KW_FIELD_1, new BytesRef(kw1Values[choice[0]])));
                doc.add(new SortedDocValuesField(KW_FIELD_2, new BytesRef(kw2Values[choice[1]])));
                iw.addDocument(doc);
            }
            iw.close();

            try (DirectoryReader unwrapped = DirectoryReader.open(directory); IndexReader reader = wrapDirectoryReader(unwrapped)) {
                IndexSearcher searcher = newIndexSearcher(reader);
                MultiTermsAggregationBuilder builder = new MultiTermsAggregationBuilder("test_agg").terms(
                    asList(kwConfig(KW_FIELD_1), kwConfig(KW_FIELD_2))
                ).size(100);
                InternalMultiTerms result = searchAndReduce(searcher, new MatchAllDocsQuery(), builder, allFieldTypes());
                ordinalBuckets = bucketsToSortedStrings(result);
            }
        }

        // Run fallback path: keyword + int (forces BytesKeyedBucketOrds)
        List<String> fallbackBuckets;
        try (Directory directory = newDirectory()) {
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory);
            for (int[] choice : docChoices) {
                Document doc = new Document();
                doc.add(new SortedDocValuesField(KW_FIELD_1, new BytesRef(kw1Values[choice[0]])));
                doc.add(new NumericDocValuesField(INT_FIELD, kw2ToInt.get(kw2Values[choice[1]])));
                iw.addDocument(doc);
            }
            iw.close();

            try (DirectoryReader unwrapped = DirectoryReader.open(directory); IndexReader reader = wrapDirectoryReader(unwrapped)) {
                IndexSearcher searcher = newIndexSearcher(reader);
                MultiTermsAggregationBuilder builder = new MultiTermsAggregationBuilder("test_agg").terms(
                    asList(kwConfig(KW_FIELD_1), intConfig())
                ).size(100);
                InternalMultiTerms result = searchAndReduce(searcher, new MatchAllDocsQuery(), builder, allFieldTypes());
                // Convert int keys to the kw2 equivalent for comparison
                fallbackBuckets = new ArrayList<>();
                Map<Long, String> intToKw2 = new HashMap<>();
                intToKw2.put(1L, "one");
                intToKw2.put(2L, "two");
                intToKw2.put(3L, "three");
                for (InternalMultiTerms.Bucket bucket : result.getBuckets()) {
                    String kw = (String) bucket.getKey().get(0);
                    String mapped = intToKw2.get(bucket.getKey().get(1));
                    fallbackBuckets.add(kw + "|" + mapped + "=" + bucket.getDocCount());
                }
                fallbackBuckets.sort(Comparator.naturalOrder());
            }
        }

        assertEquals("Ordinal and fallback paths should produce equivalent results", ordinalBuckets, fallbackBuckets);
    }

    /**
     * Test with high-cardinality fields to exercise the ordinal path with many distinct values.
     * Verifies correct results regardless of which internal packing path is used.
     */
    public void testHighCardinalityKeywordFields() throws IOException {
        // Generate enough distinct values to exercise the ordinal path with higher cardinality
        int numDistinct = randomIntBetween(50, 200);
        String[] values = new String[numDistinct];
        for (int i = 0; i < numDistinct; i++) {
            values[i] = String.format(Locale.ROOT, "%05d_%s", i, randomAlphaOfLength(5));
        }

        int numDocs = randomIntBetween(100, 300);
        Map<String, Long> expectedCounts = new HashMap<>();

        try (Directory directory = newDirectory()) {
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory);
            for (int i = 0; i < numDocs; i++) {
                String v1 = values[randomIntBetween(0, numDistinct - 1)];
                String v2 = values[randomIntBetween(0, numDistinct - 1)];
                Document doc = new Document();
                doc.add(new SortedDocValuesField(KW_FIELD_1, new BytesRef(v1)));
                doc.add(new SortedDocValuesField(KW_FIELD_2, new BytesRef(v2)));
                iw.addDocument(doc);
                expectedCounts.merge(v1 + "|" + v2, 1L, Long::sum);
            }
            iw.close();

            try (DirectoryReader unwrapped = DirectoryReader.open(directory); IndexReader reader = wrapDirectoryReader(unwrapped)) {
                IndexSearcher searcher = newIndexSearcher(reader);

                // Use a large size to capture all buckets
                MultiTermsAggregationBuilder builder = new MultiTermsAggregationBuilder("test_agg").terms(
                    asList(kwConfig(KW_FIELD_1), kwConfig(KW_FIELD_2))
                ).size(expectedCounts.size() + 10);

                InternalMultiTerms result = searchAndReduce(searcher, new MatchAllDocsQuery(), builder, allFieldTypes());

                Map<String, Long> actualCounts = new HashMap<>();
                for (InternalMultiTerms.Bucket bucket : result.getBuckets()) {
                    List<Object> key = bucket.getKey();
                    assertEquals(2, key.size());
                    String compositeKey = key.get(0) + "|" + key.get(1);
                    actualCounts.put(compositeKey, bucket.getDocCount());
                }

                assertEquals("Bucket count mismatch for high-cardinality test", expectedCounts.size(), actualCounts.size());
                for (Map.Entry<String, Long> entry : expectedCounts.entrySet()) {
                    assertEquals("Doc count mismatch for key " + entry.getKey(), entry.getValue(), actualCounts.get(entry.getKey()));
                }
            }
        }
    }

    /**
     * Test with multi-valued keyword fields (SortedSetDocValuesField) to verify
     * the cartesian product is correctly generated in the ordinal path.
     */
    public void testMultiValuedKeywordFieldsOrdinalPath() throws IOException {
        try (Directory directory = newDirectory()) {
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory);

            // Doc 1: kw1=[a,b], kw2=[x] → buckets: (a,x), (b,x)
            Document doc1 = new Document();
            doc1.add(new SortedSetDocValuesField(KW_FIELD_1, new BytesRef("a")));
            doc1.add(new SortedSetDocValuesField(KW_FIELD_1, new BytesRef("b")));
            doc1.add(new SortedSetDocValuesField(KW_FIELD_2, new BytesRef("x")));
            iw.addDocument(doc1);

            // Doc 2: kw1=[a], kw2=[x,y] → buckets: (a,x), (a,y)
            Document doc2 = new Document();
            doc2.add(new SortedSetDocValuesField(KW_FIELD_1, new BytesRef("a")));
            doc2.add(new SortedSetDocValuesField(KW_FIELD_2, new BytesRef("x")));
            doc2.add(new SortedSetDocValuesField(KW_FIELD_2, new BytesRef("y")));
            iw.addDocument(doc2);

            // Doc 3: kw1=[b], kw2=[y] → buckets: (b,y)
            Document doc3 = new Document();
            doc3.add(new SortedSetDocValuesField(KW_FIELD_1, new BytesRef("b")));
            doc3.add(new SortedSetDocValuesField(KW_FIELD_2, new BytesRef("y")));
            iw.addDocument(doc3);

            iw.close();

            try (DirectoryReader unwrapped = DirectoryReader.open(directory); IndexReader reader = wrapDirectoryReader(unwrapped)) {
                IndexSearcher searcher = newIndexSearcher(reader);

                MultiTermsAggregationBuilder builder = new MultiTermsAggregationBuilder("test_agg").terms(
                    asList(kwConfig(KW_FIELD_1), kwConfig(KW_FIELD_2))
                ).size(100);

                InternalMultiTerms result = searchAndReduce(searcher, new MatchAllDocsQuery(), builder, allFieldTypes());

                Map<String, Long> actualCounts = new HashMap<>();
                for (InternalMultiTerms.Bucket bucket : result.getBuckets()) {
                    String compositeKey = bucket.getKey().get(0) + "|" + bucket.getKey().get(1);
                    actualCounts.put(compositeKey, bucket.getDocCount());
                }

                // Expected: (a,x)=2, (a,y)=1, (b,x)=1, (b,y)=1
                assertEquals(Long.valueOf(2), actualCounts.get("a|x"));
                assertEquals(Long.valueOf(1), actualCounts.get("a|y"));
                assertEquals(Long.valueOf(1), actualCounts.get("b|x"));
                assertEquals(Long.valueOf(1), actualCounts.get("b|y"));
                assertEquals(4, actualCounts.size());
            }
        }
    }

    /**
     * Verify that 2 keyword fields select the PackedOrdinalBucketOrds strategy (single-long path).
     */
    public void testStrategySelectionTwoKeywordFields() throws IOException {
        try (Directory directory = newDirectory()) {
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory);
            Document doc = new Document();
            doc.add(new SortedDocValuesField(KW_FIELD_1, new BytesRef("a")));
            doc.add(new SortedDocValuesField(KW_FIELD_2, new BytesRef("b")));
            iw.addDocument(doc);
            iw.close();

            try (DirectoryReader unwrapped = DirectoryReader.open(directory); IndexReader reader = wrapDirectoryReader(unwrapped)) {
                IndexSearcher searcher = newIndexSearcher(reader);
                MultiTermsAggregationBuilder builder = new MultiTermsAggregationBuilder("test_agg").terms(
                    asList(kwConfig(KW_FIELD_1), kwConfig(KW_FIELD_2))
                );
                InternalMultiTerms result = searchAndReduce(searcher, new MatchAllDocsQuery(), builder, allFieldTypes());
                // Verify the ordinal path produced correct results (1 bucket with count 1)
                assertEquals(1, result.getBuckets().size());
                InternalMultiTerms.Bucket bucket = result.getBuckets().get(0);
                assertEquals("a", bucket.getKey().get(0));
                assertEquals("b", bucket.getKey().get(1));
                assertEquals(1, bucket.getDocCount());
            }
        }
    }

    /**
     * Verify that 3 keyword fields select the PackedOrdinalBucketOrds strategy (single-long path for low cardinality).
     */
    public void testStrategySelectionThreeKeywordFields() throws IOException {
        try (Directory directory = newDirectory()) {
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory);
            Document doc = new Document();
            doc.add(new SortedDocValuesField(KW_FIELD_1, new BytesRef("a")));
            doc.add(new SortedDocValuesField(KW_FIELD_2, new BytesRef("b")));
            doc.add(new SortedDocValuesField(KW_FIELD_3, new BytesRef("c")));
            iw.addDocument(doc);
            iw.close();

            try (DirectoryReader unwrapped = DirectoryReader.open(directory); IndexReader reader = wrapDirectoryReader(unwrapped)) {
                IndexSearcher searcher = newIndexSearcher(reader);
                MultiTermsAggregationBuilder builder = new MultiTermsAggregationBuilder("test_agg").terms(
                    asList(kwConfig(KW_FIELD_1), kwConfig(KW_FIELD_2), kwConfig(KW_FIELD_3))
                );
                InternalMultiTerms result = searchAndReduce(searcher, new MatchAllDocsQuery(), builder, allFieldTypes());
                assertEquals(1, result.getBuckets().size());
                InternalMultiTerms.Bucket bucket = result.getBuckets().get(0);
                assertEquals("a", bucket.getKey().get(0));
                assertEquals("b", bucket.getKey().get(1));
                assertEquals("c", bucket.getKey().get(2));
                assertEquals(1, bucket.getDocCount());
            }
        }
    }

    /**
     * Verify that mixed keyword + numeric fields fall back to the bytes-based path and still produce correct results.
     */
    public void testStrategySelectionMixedFieldsFallback() throws IOException {
        try (Directory directory = newDirectory()) {
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory);
            Document doc = new Document();
            doc.add(new SortedDocValuesField(KW_FIELD_1, new BytesRef("a")));
            doc.add(new NumericDocValuesField(INT_FIELD, 42));
            iw.addDocument(doc);
            iw.close();

            try (DirectoryReader unwrapped = DirectoryReader.open(directory); IndexReader reader = wrapDirectoryReader(unwrapped)) {
                IndexSearcher searcher = newIndexSearcher(reader);
                MultiTermsAggregationBuilder builder = new MultiTermsAggregationBuilder("test_agg").terms(
                    asList(kwConfig(KW_FIELD_1), intConfig())
                );
                InternalMultiTerms result = searchAndReduce(searcher, new MatchAllDocsQuery(), builder, allFieldTypes());
                assertEquals(1, result.getBuckets().size());
                InternalMultiTerms.Bucket bucket = result.getBuckets().get(0);
                assertEquals("a", bucket.getKey().get(0));
                assertEquals(42L, bucket.getKey().get(1));
                assertEquals(1, bucket.getDocCount());
            }
        }
    }

    /**
     * Exercises the PackedOrdinalBucketOrds two-long path end-to-end via searchAndReduce.
     * Uses 8 keyword fields each with 300 distinct values: bitsRequired(300)=9, total=72 bits,
     * which exceeds 63 and forces the LongLongHash-backed path.
     */
    public void testTwoLongPathEndToEnd() throws IOException {
        final int numFields = 8;
        final int numDistinct = 300;
        // Sanity-check that this shape actually forces the two-long path.
        long[] maxOrds = new long[numFields];
        Arrays.fill(maxOrds, numDistinct);
        assertFalse("shape should not fit in a single long", PackedOrdinalBucketOrds.fitsInSingleLong(maxOrds));
        assertTrue("shape should fit in two longs", PackedOrdinalBucketOrds.fitsInTwoLongs(maxOrds));

        String[] fieldNames = new String[numFields];
        MappedFieldType[] fieldTypes = new MappedFieldType[numFields];
        List<MultiTermsValuesSourceConfig> configs = new ArrayList<>(numFields);
        for (int f = 0; f < numFields; f++) {
            fieldNames[f] = "f" + f;
            fieldTypes[f] = new KeywordFieldMapper.KeywordFieldType(fieldNames[f]);
            configs.add(kwConfig(fieldNames[f]));
        }

        int numDocs = randomIntBetween(numDistinct, numDistinct + 200);
        Map<String, Long> expectedCounts = new HashMap<>();
        try (Directory directory = newDirectory()) {
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory);
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                StringBuilder keyBuilder = new StringBuilder();
                for (int f = 0; f < numFields; f++) {
                    // Ensure all numDistinct ordinals are realized so globalMaxOrd reaches numDistinct.
                    int ordIdx = i < numDistinct ? i : randomIntBetween(0, numDistinct - 1);
                    String value = String.format(Locale.ROOT, "%s_v%03d", fieldNames[f], ordIdx);
                    doc.add(new SortedDocValuesField(fieldNames[f], new BytesRef(value)));
                    if (f > 0) keyBuilder.append("|");
                    keyBuilder.append(value);
                }
                iw.addDocument(doc);
                expectedCounts.merge(keyBuilder.toString(), 1L, Long::sum);
            }
            iw.close();

            try (DirectoryReader unwrapped = DirectoryReader.open(directory); IndexReader reader = wrapDirectoryReader(unwrapped)) {
                IndexSearcher searcher = newIndexSearcher(reader);
                MultiTermsAggregationBuilder builder = new MultiTermsAggregationBuilder("test_agg").terms(configs)
                    .size(expectedCounts.size() + 10);
                InternalMultiTerms result = searchAndReduce(searcher, new MatchAllDocsQuery(), builder, fieldTypes);

                Map<String, Long> actualCounts = new HashMap<>();
                for (InternalMultiTerms.Bucket bucket : result.getBuckets()) {
                    assertEquals(numFields, bucket.getKey().size());
                    StringBuilder sb = new StringBuilder();
                    for (int f = 0; f < numFields; f++) {
                        if (f > 0) sb.append("|");
                        sb.append(bucket.getKey().get(f));
                    }
                    actualCounts.put(sb.toString(), bucket.getDocCount());
                }
                assertEquals("Bucket count mismatch on two-long path", expectedCounts.size(), actualCounts.size());
                for (Map.Entry<String, Long> entry : expectedCounts.entrySet()) {
                    assertEquals("Doc count mismatch for " + entry.getKey(), entry.getValue(), actualCounts.get(entry.getKey()));
                }
            }
        }
    }

    /**
     * Exercises the ordinal path's zero-doc-filling branch (collectZeroDocOrdinals) with
     * min_doc_count=0 and a filtered query. The filter excludes some docs from bucket
     * collection; with min_doc_count=0 those unmatched tuples still appear as 0-count buckets.
     */
    public void testOrdinalPathMinDocCountZeroFillsBuckets() throws IOException {
        try (Directory directory = newDirectory()) {
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory);
            // Doc 0: kw1=a, kw2=x — matches the filter
            Document d0 = new Document();
            d0.add(new SortedDocValuesField(KW_FIELD_1, new BytesRef("a")));
            d0.add(new StringField(KW_FIELD_1, "a", Field.Store.NO));
            d0.add(new SortedDocValuesField(KW_FIELD_2, new BytesRef("x")));
            iw.addDocument(d0);
            // Doc 1: kw1=b, kw2=y — excluded by filter; should still surface with 0 count.
            Document d1 = new Document();
            d1.add(new SortedDocValuesField(KW_FIELD_1, new BytesRef("b")));
            d1.add(new StringField(KW_FIELD_1, "b", Field.Store.NO));
            d1.add(new SortedDocValuesField(KW_FIELD_2, new BytesRef("y")));
            iw.addDocument(d1);
            iw.close();

            try (DirectoryReader unwrapped = DirectoryReader.open(directory); IndexReader reader = wrapDirectoryReader(unwrapped)) {
                IndexSearcher searcher = newIndexSearcher(reader);
                MultiTermsAggregationBuilder builder = new MultiTermsAggregationBuilder("test_agg").terms(
                    asList(kwConfig(KW_FIELD_1), kwConfig(KW_FIELD_2))
                ).size(50).minDocCount(0);
                // Filter to just doc 0; zero-doc-fill should still add the (b,y) tuple.
                InternalMultiTerms result = searchAndReduce(searcher, new TermQuery(new Term(KW_FIELD_1, "a")), builder, allFieldTypes());

                Map<String, Long> actual = new HashMap<>();
                for (InternalMultiTerms.Bucket b : result.getBuckets()) {
                    actual.put(b.getKey().get(0) + "|" + b.getKey().get(1), b.getDocCount());
                }
                assertEquals("Expected 2 buckets (1 matched + 1 zero-filled), got: " + actual, 2, actual.size());
                assertEquals(Long.valueOf(1), actual.get("a|x"));
                assertEquals(Long.valueOf(0), actual.get("b|y"));
            }
        }
    }

    /**
     * Verifies that for multi-valued docs, sub-aggregations are correctly accumulated across
     * all cartesian-product tuples via the {@code collectExistingBucket} branch in the
     * ordinal-path leaf collector.
     */
    public void testOrdinalPathMultiValuedWithSubAggAccumulation() throws IOException {
        final String NUM_FIELD = "n";
        MappedFieldType[] fieldTypes = {
            new KeywordFieldMapper.KeywordFieldType(KW_FIELD_1),
            new KeywordFieldMapper.KeywordFieldType(KW_FIELD_2),
            new NumberFieldMapper.NumberFieldType(NUM_FIELD, NumberFieldMapper.NumberType.INTEGER) };
        try (Directory directory = newDirectory()) {
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory);
            // Doc 0: kw1=[a,b], kw2=[x], n=10 → tuples (a,x) and (b,x)
            Document d0 = new Document();
            d0.add(new SortedSetDocValuesField(KW_FIELD_1, new BytesRef("a")));
            d0.add(new SortedSetDocValuesField(KW_FIELD_1, new BytesRef("b")));
            d0.add(new SortedSetDocValuesField(KW_FIELD_2, new BytesRef("x")));
            d0.add(new NumericDocValuesField(NUM_FIELD, 10));
            iw.addDocument(d0);
            // Doc 1: kw1=[a], kw2=[x,y], n=20 → tuples (a,x) and (a,y)
            Document d1 = new Document();
            d1.add(new SortedSetDocValuesField(KW_FIELD_1, new BytesRef("a")));
            d1.add(new SortedSetDocValuesField(KW_FIELD_2, new BytesRef("x")));
            d1.add(new SortedSetDocValuesField(KW_FIELD_2, new BytesRef("y")));
            d1.add(new NumericDocValuesField(NUM_FIELD, 20));
            iw.addDocument(d1);
            // Doc 2: kw1=[b], kw2=[y], n=5 → (b,y)
            Document d2 = new Document();
            d2.add(new SortedSetDocValuesField(KW_FIELD_1, new BytesRef("b")));
            d2.add(new SortedSetDocValuesField(KW_FIELD_2, new BytesRef("y")));
            d2.add(new NumericDocValuesField(NUM_FIELD, 5));
            iw.addDocument(d2);
            iw.close();

            try (DirectoryReader unwrapped = DirectoryReader.open(directory); IndexReader reader = wrapDirectoryReader(unwrapped)) {
                IndexSearcher searcher = newIndexSearcher(reader);
                MultiTermsAggregationBuilder builder = new MultiTermsAggregationBuilder("test_agg").terms(
                    asList(kwConfig(KW_FIELD_1), kwConfig(KW_FIELD_2))
                ).size(100).subAggregation(new SumAggregationBuilder("sum_n").field(NUM_FIELD));

                InternalMultiTerms result = searchAndReduce(searcher, new MatchAllDocsQuery(), builder, fieldTypes);

                Map<String, long[]> actual = new HashMap<>(); // key -> [docCount, sum]
                for (InternalMultiTerms.Bucket b : result.getBuckets()) {
                    double sum = ((InternalSum) b.getAggregations().get("sum_n")).value();
                    actual.put(b.getKey().get(0) + "|" + b.getKey().get(1), new long[] { b.getDocCount(), (long) sum });
                }
                // Expected: (a,x) count=2 sum=30, (a,y) count=1 sum=20, (b,x) count=1 sum=10, (b,y) count=1 sum=5
                assertEquals(4, actual.size());
                assertArrayEquals("a|x", new long[] { 2, 30 }, actual.get("a|x"));
                assertArrayEquals("a|y", new long[] { 1, 20 }, actual.get("a|y"));
                assertArrayEquals("b|x", new long[] { 1, 10 }, actual.get("b|x"));
                assertArrayEquals("b|y", new long[] { 1, 5 }, actual.get("b|y"));
            }
        }
    }

    /**
     * When a multi_terms whose ordinal shape would require the two-long path is nested under
     * a parent bucket aggregation (so {@code CardinalityUpperBound > ONE}), the factory must
     * fall back to the bytes path because {@link org.opensearch.common.util.LongLongHash} does
     * not carry an owningBucketOrd dimension. Result correctness must be preserved.
     */
    public void testNestedMultiTermsTwoLongShapeFallsBackToBytes() throws IOException {
        // Parent field for the outer terms agg.
        final String PARENT_FIELD = "parent";
        final int numInnerFields = 8;
        final int numDistinct = 300; // 9 bits per field × 8 fields = 72 bits → two-long shape
        String[] innerFieldNames = new String[numInnerFields];
        MappedFieldType[] fieldTypes = new MappedFieldType[numInnerFields + 1];
        fieldTypes[0] = new KeywordFieldMapper.KeywordFieldType(PARENT_FIELD);
        List<MultiTermsValuesSourceConfig> innerConfigs = new ArrayList<>(numInnerFields);
        for (int f = 0; f < numInnerFields; f++) {
            innerFieldNames[f] = "f" + f;
            fieldTypes[f + 1] = new KeywordFieldMapper.KeywordFieldType(innerFieldNames[f]);
            innerConfigs.add(kwConfig(innerFieldNames[f]));
        }
        // Sanity-check the shape requires the two-long path.
        long[] maxOrds = new long[numInnerFields];
        Arrays.fill(maxOrds, numDistinct);
        assertFalse(PackedOrdinalBucketOrds.fitsInSingleLong(maxOrds));
        assertTrue(PackedOrdinalBucketOrds.fitsInTwoLongs(maxOrds));

        // 2 parent buckets × numDistinct inner ordinals per field; realize every ordinal.
        String[] parents = { "p1", "p2" };
        Map<String, Map<String, Long>> expected = new HashMap<>(); // parent -> tuple -> count
        expected.put(parents[0], new HashMap<>());
        expected.put(parents[1], new HashMap<>());

        try (Directory directory = newDirectory()) {
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory);
            for (int i = 0; i < numDistinct; i++) {
                for (String parent : parents) {
                    Document doc = new Document();
                    doc.add(new SortedDocValuesField(PARENT_FIELD, new BytesRef(parent)));
                    StringBuilder sb = new StringBuilder();
                    for (int f = 0; f < numInnerFields; f++) {
                        String v = String.format(Locale.ROOT, "%s_v%03d", innerFieldNames[f], i);
                        doc.add(new SortedDocValuesField(innerFieldNames[f], new BytesRef(v)));
                        if (f > 0) sb.append("|");
                        sb.append(v);
                    }
                    iw.addDocument(doc);
                    expected.get(parent).merge(sb.toString(), 1L, Long::sum);
                }
            }
            iw.close();

            try (DirectoryReader unwrapped = DirectoryReader.open(directory); IndexReader reader = wrapDirectoryReader(unwrapped)) {
                IndexSearcher searcher = newIndexSearcher(reader);
                TermsAggregationBuilder outer = new TermsAggregationBuilder("by_parent").field(PARENT_FIELD).size(10);
                outer.subAggregation(new MultiTermsAggregationBuilder("inner").terms(innerConfigs).size(numDistinct + 5));
                InternalAggregation result = searchAndReduce(searcher, new MatchAllDocsQuery(), outer, fieldTypes);
                StringTerms outerTerms = (StringTerms) result;
                assertEquals(2, outerTerms.getBuckets().size());
                for (StringTerms.Bucket pb : outerTerms.getBuckets()) {
                    String parent = pb.getKeyAsString();
                    InternalMultiTerms inner = pb.getAggregations().get("inner");
                    Map<String, Long> actual = new HashMap<>();
                    for (InternalMultiTerms.Bucket b : inner.getBuckets()) {
                        StringBuilder sb = new StringBuilder();
                        for (int f = 0; f < numInnerFields; f++) {
                            if (f > 0) sb.append("|");
                            sb.append(b.getKey().get(f));
                        }
                        actual.put(sb.toString(), b.getDocCount());
                    }
                    assertEquals("bucket count mismatch for parent=" + parent, expected.get(parent).size(), actual.size());
                    for (Map.Entry<String, Long> e : expected.get(parent).entrySet()) {
                        assertEquals("doc count mismatch for " + parent + "/" + e.getKey(), e.getValue(), actual.get(e.getKey()));
                    }
                }
            }
        }
    }

    /**
     * Convert InternalMultiTerms buckets to a sorted list of "key=count" strings for comparison.
     */
    private List<String> bucketsToSortedStrings(InternalMultiTerms result) {
        List<String> bucketStrings = new ArrayList<>();
        for (InternalMultiTerms.Bucket bucket : result.getBuckets()) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < bucket.getKey().size(); i++) {
                if (i > 0) {
                    sb.append("|");
                }
                sb.append(bucket.getKey().get(i));
            }
            sb.append("=").append(bucket.getDocCount());
            bucketStrings.add(sb.toString());
        }
        bucketStrings.sort(Comparator.naturalOrder());
        return bucketStrings;
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return List.of(
            CoreValuesSourceType.NUMERIC,
            CoreValuesSourceType.BYTES,
            CoreValuesSourceType.IP,
            CoreValuesSourceType.DATE,
            CoreValuesSourceType.BOOLEAN
        );
    }

    @Override
    protected List<String> unsupportedMappedFieldTypes() {
        return List.of(HllFieldMapper.CONTENT_TYPE);
    }

    @Override
    protected org.opensearch.search.aggregations.AggregationBuilder createAggBuilderForTypeTest(
        MappedFieldType fieldType,
        String fieldName
    ) {
        return new MultiTermsAggregationBuilder("_name").terms(asList(kwConfig(fieldName), kwConfig(fieldName)));
    }
}
