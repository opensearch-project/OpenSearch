/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.writer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.IndexWriter;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.DocumentInput;
import org.opensearch.index.engine.exec.EngineRole;
import org.opensearch.index.engine.exec.WriteResult;
import org.opensearch.index.engine.exec.lucene.fields.LuceneField;
import org.opensearch.index.engine.exec.lucene.fields.LuceneFieldRegistry;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParseContext;

import java.io.IOException;

public class LuceneDocumentInput implements DocumentInput<ParseContext.Document> {
    private static final Logger logger = LogManager.getLogger(LuceneDocumentInput.class);
    private final ParseContext.Document document;
    private final IndexWriter indexWriter;
    private final EngineRole engineRole;

    public LuceneDocumentInput(ParseContext.Document document, IndexWriter indexWriter, EngineRole engineRole) {
        this.document = document;
        this.indexWriter = indexWriter;
        this.engineRole = engineRole;
    }

    @Override
    public void addRowIdField(String fieldName, long rowId) {
        document.add(new NumericDocValuesField(fieldName, rowId));
    }

    @SuppressWarnings("unchecked")
    @Override
    public void addField(MappedFieldType fieldType, Object value) {
        final LuceneField luceneField = LuceneFieldRegistry.getLuceneField(fieldType.typeName());

        if (luceneField == null) {
            // Field type not supported by Lucene format — skip silently
            logger.debug(
                "[COMPOSITE_DEBUG] Lucene SKIP field=[{}] type=[{}] — no LuceneField registered in LuceneFieldRegistry",
                fieldType.name(),
                fieldType.typeName()
            );
            return;
        }

        logger.debug(
            "[COMPOSITE_DEBUG] Lucene ACCEPT field=[{}] type=[{}] value=[{}]",
            fieldType.name(),
            fieldType.typeName(),
            value
        );
        luceneField.createField(fieldType, document, value);
    }

    /**
     * Returns the underlying {@link ParseContext.Document} for ingesters to access
     * and add Lucene fields directly.
     */
    public ParseContext.Document getDocument() {
        return document;
    }

    @Override
    public EngineRole getEngineRole() {
        return engineRole;
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
    public DataFormat getDataFormat() {
        return DataFormat.LUCENE;
    }

    @Override
    public void close() throws Exception {
        // no-op, reuse writer
    }

}
