/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.internal;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;

import java.util.List;

import static org.apache.lucene.tests.util.LuceneTestCase.newDirectory;

public class IndexReaderUtils {

    /**
     * Utility to create leafCount number of {@link LeafReaderContext}
     * @param leafCount count of leaves to create
     * @return created leaves
     */
    public static List<LeafReaderContext> getLeaves(int leafCount) throws Exception {
        try (
            final Directory directory = newDirectory();
            final IndexWriter iw = new IndexWriter(
                directory,
                new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {
            for (int i = 0; i < leafCount; ++i) {
                Document document = new Document();
                final String fieldValue = "value" + i;
                document.add(new StringField("field1", fieldValue, Field.Store.NO));
                document.add(new StringField("field2", fieldValue, Field.Store.NO));
                iw.addDocument(document);
                iw.commit();
            }
            try (DirectoryReader directoryReader = DirectoryReader.open(directory)) {
                List<LeafReaderContext> leaves = directoryReader.leaves();
                return leaves;
            }
        }
    }
}
