/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.mapper.FlatObjectFieldMapper.FlatObjectFieldType;
import org.opensearch.search.aggregations.AggregatorTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

public class FlatObjectTermsAggregatorTests extends AggregatorTestCase {
    private static final String ROOT = "custom_dimensions";

    public void testSubfieldTerms() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
            Document first = new Document();
            addValue(first, "request_id", "abc");
            addValue(first, "request_id1", "abc1");
            writer.addDocument(first);

            Document second = new Document();
            addValue(second, "request_id", "abc");
            addValue(second, "request_id", "xyz");
            addValue(second, "request_id1", "sibling");
            writer.addDocument(second);

            try (DirectoryReader reader = writer.getReader()) {
                FlatObjectFieldType fieldType = new FlatObjectFieldType(ROOT + ".request_id", ROOT, true, true);
                StringTerms result = searchAndReduce(
                    newIndexSearcher(reader),
                    MatchAllDocsQuery.INSTANCE,
                    new TermsAggregationBuilder("terms").field(fieldType.name()),
                    fieldType
                );
                assertEquals(
                    Map.of("abc", 2L, "xyz", 1L),
                    result.getBuckets()
                        .stream()
                        .collect(Collectors.toMap(StringTerms.Bucket::getKeyAsString, StringTerms.Bucket::getDocCount))
                );
            }
        }
    }

    private static void addValue(Document document, String key, String value) {
        document.add(new SortedSetDocValuesField(ROOT + "._valueAndPath", new BytesRef(ROOT + "." + ROOT + "." + key + "=" + value)));
    }
}
