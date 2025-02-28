/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.filterrewrite;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.DocIdSetBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class DocIdSetBuilderTests extends OpenSearchTestCase {

    private Directory directory;
    private IndexWriter iw;
    private DirectoryReader reader;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        directory = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig();
        iw = new IndexWriter(directory, iwc);

        // Add some test documents
        for (int i = 0; i < 100; i++) {
            Document doc = new Document();
            doc.add(new StringField("text_field", "value_" + i, Field.Store.NO));
            doc.add(new NumericDocValuesField("numeric_field", i));
            doc.add(new BinaryDocValuesField("binary_field", new BytesRef("value_" + i)));
            iw.addDocument(doc);
        }
        iw.commit();
        reader = DirectoryReader.open(iw);
    }

    @Override
    public void tearDown() throws Exception {
        reader.close();
        iw.close();
        directory.close();
        super.tearDown();
    }

    public void testBasicDocIdSetBuilding() throws IOException {
        LeafReaderContext context = reader.leaves().get(0);

        // Test with different maxDoc sizes
        DocIdSetBuilder builder = new DocIdSetBuilder(context.reader().maxDoc());
        DocIdSetBuilder.BulkAdder adder = builder.grow(10);

        adder.add(0);
        adder.add(5);
        adder.add(10);

        DocIdSet docIdSet = builder.build();
        assertNotNull(docIdSet);

        DocIdSetIterator iterator = docIdSet.iterator();
        assertNotNull(iterator);

        assertEquals(0, iterator.nextDoc());
        assertEquals(5, iterator.nextDoc());
        assertEquals(10, iterator.nextDoc());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.nextDoc());
    }
}
