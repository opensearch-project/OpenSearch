/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.DocumentInput;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.FlushIn;
import org.opensearch.index.engine.exec.IndexingExecutionEngine;
import org.opensearch.index.engine.exec.RefreshInput;
import org.opensearch.index.engine.exec.RefreshResult;
import org.opensearch.index.engine.exec.WriteResult;
import org.opensearch.index.engine.exec.Writer;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParseContext;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class LuceneIEEngine implements IndexingExecutionEngine<DataFormat.LuceneDataFormat> {

    private final InternalEngine internalEngine;

    public LuceneIEEngine(InternalEngine internalEngine) {
        this.internalEngine = internalEngine;
    }

    @Override
    public List<String> supportedFieldTypes() {
        return List.of();
    }

    @Override
    public Writer<? extends DocumentInput<?>> createWriter() throws IOException {
        return new LuceneWriter(internalEngine.indexWriter);
    }

    @Override
    public RefreshResult refresh(RefreshInput refreshInput) throws IOException {
        internalEngine.refresh(refreshInput.getClass().getName());
        return null;
    }

    @Override
    public DataFormat getDataFormat() {
        return DataFormat.LUCENE;
    }


    public static class LuceneDocumentInput implements DocumentInput<ParseContext.Document> {

        private final ParseContext.Document doc;
        private final IndexWriter writer;

        public LuceneDocumentInput(ParseContext.Document doc, IndexWriter w) {
            this.doc = doc;
            this.writer = w;
        }

        @Override
        public void addField(MappedFieldType fieldType, Object value) {
            doc.add(new KeywordFieldMapper.KeywordField("f1", new BytesRef("good_field"), null));
        }

        @Override
        public ParseContext.Document getFinalInput() {
            return doc;
        }

        @Override
        public WriteResult addToWriter() throws IOException {
            writer.addDocument(doc);
            return null;
        }

        @Override
        public void close() throws Exception {
            // no-op, reuse writer
        }
    }

    public static class LuceneWriter implements Writer<LuceneDocumentInput> {

        private IndexWriter writer;

        public LuceneWriter(IndexWriter writer) {
            this.writer = writer;
        }

        @Override
        public WriteResult addDoc(LuceneDocumentInput d) throws IOException {
            writer.addDocument(d.doc);
            return null;
        }

        @Override
        public FileMetadata flush(FlushIn flushIn) throws IOException {
            writer.flush();
            return null;
        }

        @Override
        public void sync() throws IOException {
            writer.flush();
        }

        @Override
        public void close() {
            // no-op
        }

        @Override
        public Optional<FileMetadata> getMetadata() {
            return Optional.empty();
        }

        @Override
        public LuceneDocumentInput newDocumentInput() {
            return new LuceneDocumentInput(new ParseContext.Document(), writer);
        }
    }
}
