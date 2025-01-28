/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.ParsedAggregation;
import org.opensearch.test.InternalAggregationTestCase;
import org.opensearch.test.NotEqualMessageBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

public class InternalTopHitsTests extends InternalAggregationTestCase<InternalTopHits> {
    @Override
    protected InternalTopHits createTestInstance(String name, Map<String, Object> metadata) {
        if (randomBoolean()) {
            return createTestInstanceSortedByFields(
                name,
                between(1, 40),
                metadata,
                OpenSearchTestCase::randomFloat,
                randomSortFields(),
                InternalTopHitsTests::randomOfType
            );
        }
        return createTestInstanceSortedScore(name, between(1, 40), metadata, OpenSearchTestCase::randomFloat);
    }

    @Override
    protected List<InternalTopHits> randomResultsToReduce(String name, int size) {
        /*
         * Make sure all scores are unique so we can get
         * deterministic test results.
         */
        Set<Float> usedScores = new HashSet<>();
        Supplier<Float> scoreSupplier = () -> {
            float score = randomValueOtherThanMany(usedScores::contains, OpenSearchTestCase::randomFloat);
            usedScores.add(score);
            return score;
        };
        int requestedSize = between(1, 40);
        Supplier<InternalTopHits> supplier;
        if (randomBoolean()) {
            SortField[] sortFields = randomSortFields();
            Set<Object> usedSortFieldValues = new HashSet<>();
            Function<SortField.Type, Object> sortFieldValueSuppier = t -> {
                Object value = randomValueOtherThanMany(usedSortFieldValues::contains, () -> randomOfType(t));
                usedSortFieldValues.add(value);
                return value;
            };
            supplier = () -> createTestInstanceSortedByFields(name, requestedSize, null, scoreSupplier, sortFields, sortFieldValueSuppier);
        } else {
            supplier = () -> createTestInstanceSortedScore(name, requestedSize, null, scoreSupplier);
        }
        return Stream.generate(supplier).limit(size).collect(toList());
    }

    private InternalTopHits createTestInstanceSortedByFields(
        String name,
        int requestedSize,
        Map<String, Object> metadata,
        Supplier<Float> scoreSupplier,
        SortField[] sortFields,
        Function<SortField.Type, Object> sortFieldValueSupplier
    ) {
        return createTestInstance(name, metadata, scoreSupplier, requestedSize, (docId, score) -> {
            Object[] fields = new Object[sortFields.length];
            for (int f = 0; f < sortFields.length; f++) {
                final int ff = f;
                fields[f] = sortFieldValueSupplier.apply(sortFields[ff].getType());
            }
            return new FieldDoc(docId, score, fields);
        }, (totalHits, scoreDocs) -> new TopFieldDocs(totalHits, scoreDocs, sortFields), sortFieldsComparator(sortFields));
    }

    private InternalTopHits createTestInstanceSortedScore(
        String name,
        int requestedSize,
        Map<String, Object> metadata,
        Supplier<Float> scoreSupplier
    ) {
        return createTestInstance(name, metadata, scoreSupplier, requestedSize, ScoreDoc::new, TopDocs::new, scoreComparator());
    }

    private InternalTopHits createTestInstance(
        String name,
        Map<String, Object> metadata,
        Supplier<Float> scoreSupplier,
        int requestedSize,
        BiFunction<Integer, Float, ScoreDoc> docBuilder,
        BiFunction<TotalHits, ScoreDoc[], TopDocs> topDocsBuilder,
        Comparator<ScoreDoc> comparator
    ) {
        int from = 0;
        int actualSize = between(0, requestedSize);

        float maxScore = Float.NEGATIVE_INFINITY;
        ScoreDoc[] scoreDocs = new ScoreDoc[actualSize];
        SearchHit[] hits = new SearchHit[actualSize];
        Set<Integer> usedDocIds = new HashSet<>();
        for (int i = 0; i < actualSize; i++) {
            float score = scoreSupplier.get();
            maxScore = max(maxScore, score);
            int docId = randomValueOtherThanMany(usedDocIds::contains, () -> between(0, IndexWriter.MAX_DOCS));
            usedDocIds.add(docId);

            Map<String, DocumentField> searchHitFields = new HashMap<>();
            scoreDocs[i] = docBuilder.apply(docId, score);
            hits[i] = new SearchHit(docId, Integer.toString(i), searchHitFields, Collections.emptyMap());
            hits[i].score(score);
        }
        int totalHits = between(actualSize, 500000);
        sort(hits, scoreDocs, comparator);
        SearchHits searchHits = new SearchHits(hits, new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), maxScore);

        TopDocs topDocs = topDocsBuilder.apply(new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), scoreDocs);
        // Lucene's TopDocs initializes the maxScore to Float.NaN, if there is no maxScore
        TopDocsAndMaxScore topDocsAndMaxScore = new TopDocsAndMaxScore(topDocs, maxScore == Float.NEGATIVE_INFINITY ? Float.NaN : maxScore);

        return new InternalTopHits(name, from, requestedSize, topDocsAndMaxScore, searchHits, metadata);
    }

    /**
     * Sorts both searchHits and scoreDocs together based on scoreDocComparator()
     */
    private void sort(SearchHit[] searchHits, ScoreDoc[] scoreDocs, Comparator<ScoreDoc> comparator) {
        List<Tuple<SearchHit, ScoreDoc>> hitScores = new ArrayList<>();
        for (int i = 0; i < searchHits.length; i++) {
            hitScores.add(new Tuple<>(searchHits[i], scoreDocs[i]));
        }
        hitScores.sort((t1, t2) -> comparator.compare(t1.v2(), t2.v2()));
        for (int i = 0; i < searchHits.length; i++) {
            searchHits[i] = hitScores.get(i).v1();
            scoreDocs[i] = hitScores.get(i).v2();
        }
    }

    @Override
    protected void assertFromXContent(InternalTopHits aggregation, ParsedAggregation parsedAggregation) throws IOException {
        final SearchHits expectedSearchHits = aggregation.getHits();

        assertTrue(parsedAggregation instanceof ParsedTopHits);
        ParsedTopHits parsed = (ParsedTopHits) parsedAggregation;
        final SearchHits actualSearchHits = parsed.getHits();

        assertEquals(expectedSearchHits.getTotalHits().value(), actualSearchHits.getTotalHits().value());
        assertEquals(expectedSearchHits.getTotalHits().relation(), actualSearchHits.getTotalHits().relation());
        assertEquals(expectedSearchHits.getMaxScore(), actualSearchHits.getMaxScore(), 0.0f);

        List<SearchHit> expectedHits = Arrays.asList(expectedSearchHits.getHits());
        List<SearchHit> actualHits = Arrays.asList(actualSearchHits.getHits());

        assertEquals(expectedHits.size(), actualHits.size());
        for (int i = 0; i < expectedHits.size(); i++) {
            SearchHit expected = expectedHits.get(i);
            SearchHit actual = actualHits.get(i);

            assertEquals(expected.getIndex(), actual.getIndex());
            assertEquals(expected.getId(), actual.getId());
            assertEquals(expected.getVersion(), actual.getVersion());
            assertEquals(expected.getScore(), actual.getScore(), 0.0f);
            assertEquals(expected.getFields(), actual.getFields());
            assertEquals(expected.getSourceAsMap(), actual.getSourceAsMap());
        }
    }

    private static Object randomOfType(SortField.Type type) {
        switch (type) {
            case CUSTOM:
                throw new UnsupportedOperationException();
            case DOC:
                return between(0, IndexWriter.MAX_DOCS);
            case DOUBLE:
                return randomDouble();
            case FLOAT:
                return randomFloat();
            case INT:
                return randomInt();
            case LONG:
                return randomLong();
            case REWRITEABLE:
                throw new UnsupportedOperationException();
            case SCORE:
                return randomFloat();
            case STRING:
                return new BytesRef(randomAlphaOfLength(5));
            case STRING_VAL:
                return new BytesRef(randomAlphaOfLength(5));
            default:
                throw new UnsupportedOperationException("Unknown SortField.Type: " + type);
        }
    }

    @Override
    protected void assertReduced(InternalTopHits reduced, List<InternalTopHits> inputs) {
        boolean sortedByFields = inputs.get(0).getTopDocs().topDocs instanceof TopFieldDocs;
        Comparator<ScoreDoc> dataNodeComparator;
        if (sortedByFields) {
            dataNodeComparator = sortFieldsComparator(((TopFieldDocs) inputs.get(0).getTopDocs().topDocs).fields);
        } else {
            dataNodeComparator = scoreComparator();
        }
        Comparator<ScoreDoc> reducedComparator = dataNodeComparator.thenComparing(s -> s.shardIndex);
        SearchHits actualHits = reduced.getHits();
        List<Tuple<ScoreDoc, SearchHit>> allHits = new ArrayList<>();
        float maxScore = Float.NEGATIVE_INFINITY;
        long totalHits = 0;
        TotalHits.Relation relation = TotalHits.Relation.EQUAL_TO;
        for (int input = 0; input < inputs.size(); input++) {
            SearchHits internalHits = inputs.get(input).getHits();
            totalHits += internalHits.getTotalHits().value();
            if (internalHits.getTotalHits().relation() == TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO) {
                relation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
            }
            maxScore = max(maxScore, internalHits.getMaxScore());
            for (int i = 0; i < internalHits.getHits().length; i++) {
                ScoreDoc doc = inputs.get(input).getTopDocs().topDocs.scoreDocs[i];
                if (sortedByFields) {
                    doc = new FieldDoc(doc.doc, doc.score, ((FieldDoc) doc).fields, input);
                } else {
                    doc = new ScoreDoc(doc.doc, doc.score, input);
                }
                allHits.add(new Tuple<>(doc, internalHits.getHits()[i]));
            }
        }
        allHits.sort(comparing(Tuple::v1, reducedComparator));
        SearchHit[] expectedHitsHits = new SearchHit[min(inputs.get(0).getSize(), allHits.size())];
        for (int i = 0; i < expectedHitsHits.length; i++) {
            expectedHitsHits[i] = allHits.get(i).v2();
        }
        // Lucene's TopDocs initializes the maxScore to Float.NaN, if there is no maxScore
        SearchHits expectedHits = new SearchHits(
            expectedHitsHits,
            new TotalHits(totalHits, relation),
            maxScore == Float.NEGATIVE_INFINITY ? Float.NaN : maxScore
        );
        assertEqualsWithErrorMessageFromXContent(expectedHits, actualHits);
    }

    /**
     * Assert that two objects are equals, calling {@link ToXContent#toXContent(XContentBuilder, ToXContent.Params)} to print out their
     * differences if they aren't equal.
     */
    private static <T extends ToXContent> void assertEqualsWithErrorMessageFromXContent(T expected, T actual) {
        if (Objects.equals(expected, actual)) {
            return;
        }
        if (expected == null) {
            throw new AssertionError("Expected null be actual was [" + actual.toString() + "]");
        }
        if (actual == null) {
            throw new AssertionError("Didn't expect null but actual was [null]");
        }
        try (XContentBuilder actualJson = JsonXContent.contentBuilder(); XContentBuilder expectedJson = JsonXContent.contentBuilder()) {
            actualJson.startObject();
            actual.toXContent(actualJson, ToXContent.EMPTY_PARAMS);
            actualJson.endObject();
            expectedJson.startObject();
            expected.toXContent(expectedJson, ToXContent.EMPTY_PARAMS);
            expectedJson.endObject();
            NotEqualMessageBuilder message = new NotEqualMessageBuilder();
            message.compareMaps(
                XContentHelper.convertToMap(BytesReference.bytes(actualJson), false).v2(),
                XContentHelper.convertToMap(BytesReference.bytes(expectedJson), false).v2()
            );
            throw new AssertionError("Didn't match expected value:\n" + message);
        } catch (IOException e) {
            throw new AssertionError("IOException while building failure message", e);
        }
    }

    private SortField[] randomSortFields() {
        SortField[] sortFields = new SortField[between(1, 5)];
        Set<String> usedSortFields = new HashSet<>();
        for (int i = 0; i < sortFields.length; i++) {
            String sortField = randomValueOtherThanMany(usedSortFields::contains, () -> randomAlphaOfLength(5));
            usedSortFields.add(sortField);
            SortField.Type type = randomValueOtherThanMany(
                t -> t == SortField.Type.CUSTOM || t == SortField.Type.REWRITEABLE,
                () -> randomFrom(SortField.Type.values())
            );
            sortFields[i] = new SortField(sortField, type);
        }
        return sortFields;
    }

    private Comparator<ScoreDoc> sortFieldsComparator(SortField[] sortFields) {
        @SuppressWarnings("rawtypes")
        FieldComparator[] comparators = new FieldComparator[sortFields.length];
        for (int i = 0; i < sortFields.length; i++) {
            // Values passed to getComparator shouldn't matter
            comparators[i] = sortFields[i].getComparator(0, Pruning.NONE);
        }
        return (lhs, rhs) -> {
            FieldDoc l = (FieldDoc) lhs;
            FieldDoc r = (FieldDoc) rhs;
            int i = 0;
            while (i < l.fields.length) {
                @SuppressWarnings("unchecked")
                int c = comparators[i].compareValues(l.fields[i], r.fields[i]);
                if (c != 0) {
                    return c;
                }
                i++;
            }
            return 0;
        };
    }

    private Comparator<ScoreDoc> scoreComparator() {
        Comparator<ScoreDoc> comparator = comparing(d -> d.score);
        return comparator.reversed();
    }

    @Override
    protected InternalTopHits mutateInstance(InternalTopHits instance) {
        String name = instance.getName();
        int from = instance.getFrom();
        int size = instance.getSize();
        TopDocsAndMaxScore topDocs = instance.getTopDocs();
        SearchHits searchHits = instance.getHits();
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 5)) {
            case 0:
                name += randomAlphaOfLength(5);
                break;
            case 1:
                from += between(1, 100);
                break;
            case 2:
                size += between(1, 100);
                break;
            case 3:
                topDocs = new TopDocsAndMaxScore(
                    new TopDocs(
                        new TotalHits(topDocs.topDocs.totalHits.value() + between(1, 100), topDocs.topDocs.totalHits.relation()),
                        topDocs.topDocs.scoreDocs
                    ),
                    topDocs.maxScore + randomFloat()
                );
                break;
            case 4:
                TotalHits totalHits = new TotalHits(
                    searchHits.getTotalHits().value() + between(1, 100),
                    randomFrom(TotalHits.Relation.values())
                );
                searchHits = new SearchHits(searchHits.getHits(), totalHits, searchHits.getMaxScore() + randomFloat());
                break;
            case 5:
                if (metadata == null) {
                    metadata = new HashMap<>(1);
                } else {
                    metadata = new HashMap<>(instance.getMetadata());
                }
                metadata.put(randomAlphaOfLength(15), randomInt());
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalTopHits(name, from, size, topDocs, searchHits, metadata);
    }
}
