/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.mapper.HllFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.MultiTermsValuesSourceConfig;
import org.opensearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static java.util.Arrays.asList;

/**
 * End-to-end equivalence tests verifying that the ordinal-based path and the
 * bytes-based path produce identical aggregation results for the same data.
 *
 * <p><b>Property 7: Ordinal-bytes path equivalence</b></p>
 * <p><b>Validates: Requirements 5.3, 6.1, 6.2, 6.3</b></p>
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
     * Test with 2 keyword fields — exercises the OrdinalPairBucketOrds (pair packing) path.
     * Indexes random documents with 2 keyword fields, runs multi_terms aggregation,
     * and verifies results match expected bucket keys and doc counts.
     *
     * Validates: Requirements 5.3, 6.1, 6.2
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
     * Test with 3 keyword fields — exercises the PackedOrdinalsBucketOrds path.
     * Indexes random documents with 3 keyword fields, runs multi_terms aggregation,
     * and verifies results match expected bucket keys and doc counts.
     *
     * Validates: Requirements 5.3, 6.1, 6.2
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
     *
     * Validates: Requirements 6.1, 6.2, 6.3
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
     *
     * Validates: Requirements 5.3, 6.1, 6.2
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
     * Test with high-cardinality fields that could trigger pair-packing overflow.
     * When the combined bit width of two keyword fields exceeds 63 bits, the strategy
     * should fall back to PackedOrdinalsBucketOrds instead of OrdinalPairBucketOrds.
     * We simulate this by indexing many distinct values and verifying correct results.
     *
     * Validates: Requirements 5.3, 6.2
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
     *
     * Validates: Requirements 5.3, 6.2
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
     * Verify that 2 keyword fields select the OrdinalPairBucketOrds strategy.
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
                MultiTermsAggregator agg = createAggregator(builder, searcher, allFieldTypes());
                assertNotNull("Expected ordinal strategy for 2 keyword fields", agg.getOrdinalBucketOrds());
                assertTrue(
                    "Expected OrdinalPairBucketOrds for 2 keyword fields",
                    agg.getOrdinalBucketOrds() instanceof OrdinalPairBucketOrds
                );
                agg.close();
            }
        }
    }

    /**
     * Verify that 3 keyword fields select the PackedOrdinalsBucketOrds strategy.
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
                MultiTermsAggregator agg = createAggregator(builder, searcher, allFieldTypes());
                assertNotNull("Expected ordinal strategy for 3 keyword fields", agg.getOrdinalBucketOrds());
                assertTrue(
                    "Expected PackedOrdinalsBucketOrds for 3 keyword fields",
                    agg.getOrdinalBucketOrds() instanceof PackedOrdinalsBucketOrds
                );
                agg.close();
            }
        }
    }

    /**
     * Verify that mixed keyword + numeric fields fall back to the bytes-based path (no ordinal strategy).
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
                MultiTermsAggregator agg = createAggregator(builder, searcher, allFieldTypes());
                assertNull("Expected null ordinal strategy for mixed fields (bytes fallback)", agg.getOrdinalBucketOrds());
                agg.close();
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
