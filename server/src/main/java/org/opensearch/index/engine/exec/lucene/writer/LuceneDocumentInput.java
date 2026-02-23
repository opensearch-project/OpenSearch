/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.writer;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.IndexWriter;
import org.opensearch.index.engine.exec.DocumentInput;
import org.opensearch.index.engine.exec.WriteResult;
import org.opensearch.index.engine.exec.lucene.fields.LuceneField;
import org.opensearch.index.engine.exec.lucene.fields.LuceneFieldRegistry;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParseContext;

import java.io.IOException;

public class LuceneDocumentInput implements DocumentInput<ParseContext.Document> {
    private final ParseContext.Document document;
    private final IndexWriter indexWriter;

    public LuceneDocumentInput(ParseContext.Document document, IndexWriter indexWriter) {
        this.document = document;
        this.indexWriter = indexWriter;
    }

    @Override
    public void addRowIdField(String fieldName, long rowId) {
        document.add(new NumericDocValuesField(fieldName, rowId));
    }

    @Override
    public void addField(MappedFieldType fieldType, Object value) {
        final String fieldTypeName = fieldType.typeName();
        final LuceneField luceneField = LuceneFieldRegistry.getLuceneField(fieldTypeName);

        if (luceneField == null) {
            throw new IllegalArgumentException(String.format("Unsupported field type: %s. Field type is not registered in LuceneFieldRegistry.",
                fieldTypeName
            ));
        }

        luceneField.createField(fieldType, document, value);
    }

    @Override
    public ParseContext.Document getFinalInput() {
        return document;
    }

    @Override
    public WriteResult addToWriter() {
        try {
            long seqNum = indexWriter.addDocument(document);
            return new WriteResult(true, null, 1, 1, seqNum);
        } catch (IOException exception) {
            return new WriteResult(false, exception, 1, 1, 1);
        }
    }

    @Override
    public void close() throws Exception {
        // no-op, reuse writer
    }
}
