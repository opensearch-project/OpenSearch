/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.search.nested;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.opensearch.index.fielddata.IndexNumericFieldData;
import org.opensearch.index.fielddata.fieldcomparator.IntValuesComparatorSource;
import org.opensearch.search.MultiValueMode;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class IntNestedSortingTests extends AbstractNumberNestedSortingTestCase {

    @Override
    protected String getFieldDataType() {
        return "int";
    }

    @Override
    protected IndexFieldData.XFieldComparatorSource createFieldComparator(
        String fieldName,
        MultiValueMode sortMode,
        Object missingValue,
        Nested nested
    ) {
        IndexNumericFieldData fieldData = getForField(fieldName);
        return new IntValuesComparatorSource(fieldData, missingValue, sortMode, nested);
    }

    @Override
    protected IndexableField createField(String name, int value) {
        return new SortedNumericDocValuesField(name, value);
    }

    public void testNestedIntegerSortWithLimitedHitsMatchesExactSort() throws Exception {
        final int parentCount = 10_100;
        final int resultSize = 25;
        List<Document> batch = new ArrayList<>();

        for (int parentId = 0; parentId < parentCount; parentId++) {
            int childCount = parentId % 20 == 0 ? 0 : 1;
            for (int child = 0; child < childCount; child++) {
                int level = (parentId + child) % 10 < 6 ? 10 : (parentId * 31 + child * 17) % 10;
                Document childDocument = new Document();
                childDocument.add(new IntPoint("field2", level));
                childDocument.add(new SortedNumericDocValuesField("field2", level));
                batch.add(childDocument);
            }

            Document parentDocument = new Document();
            parentDocument.add(new StringField("__type", "parent", Field.Store.NO));
            parentDocument.add(new SortedDocValuesField("key", new BytesRef(String.format(Locale.ROOT, "%08x", parentId))));
            batch.add(parentDocument);
            if (parentId % 250 == 249) {
                writer.addDocuments(batch);
                batch.clear();
            }
        }
        if (batch.isEmpty() == false) {
            writer.addDocuments(batch);
        }
        writer.commit();

        DirectoryReader reader = DirectoryReader.open(writer);
        reader = OpenSearchDirectoryReader.wrap(reader, new ShardId(indexService.index(), 0));
        IndexSearcher searcher = new IndexSearcher(reader);
        TermQuery parentQuery = new TermQuery(new Term("__type", "parent"));
        Nested nested = new Nested(new QueryBitSetProducer(parentQuery), Queries.not(parentQuery), null, searcher);
        IndexNumericFieldData fieldData = getForField("field2");
        Sort sort = new Sort(fieldData.sortField(null, MultiValueMode.MAX, nested, true), new SortField("key", SortField.Type.STRING));

        TopFieldDocs limited = searcher.search(parentQuery, new TopFieldCollectorManager(sort, resultSize, 10_000));
        TopFieldDocs exact = searcher.search(parentQuery, new TopFieldCollectorManager(sort, resultSize, Integer.MAX_VALUE));

        assertEquals(resultSize, limited.scoreDocs.length);
        assertEquals(resultSize, exact.scoreDocs.length);
        for (int i = 0; i < resultSize; i++) {
            assertEquals("limited-hit mismatch at position " + i, exact.scoreDocs[i].doc, limited.scoreDocs[i].doc);
        }
        reader.close();
    }
}
