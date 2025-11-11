/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.DocumentInput;
import org.opensearch.index.engine.exec.FileInfos;
import org.opensearch.index.engine.exec.FlushIn;
import org.opensearch.index.engine.exec.IndexingExecutionEngine;
import org.opensearch.index.engine.exec.Merger;
import org.opensearch.index.engine.exec.RefreshInput;
import org.opensearch.index.engine.exec.RefreshResult;
import org.opensearch.index.engine.exec.WriteResult;
import org.opensearch.index.engine.exec.Writer;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;
import java.util.List;

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
    public Writer<? extends DocumentInput<?>> createWriter(long writerGeneration) throws IOException {
        return new LuceneWriter(internalEngine.indexWriter, writerGeneration);
    }

    @Override
    public void loadWriterFiles(ShardPath shardPath) throws IOException {

    }

    @Override
    public Merger getMerger() {
        throw new UnsupportedOperationException();
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
        public void addRowIdField(String fieldName, long rowId) {
            doc.add(new NumericDocValuesField(fieldName, rowId));
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

        private final IndexWriter writer;
        private final long writerGeneration;

        public LuceneWriter(IndexWriter writer, long writerGeneration) {
            this.writer = writer;
            this.writerGeneration = writerGeneration;
        }

        @Override
        public WriteResult addDoc(LuceneDocumentInput d) throws IOException {
            writer.addDocument(d.doc);
            return null;
        }

        @Override
        public FileInfos flush(FlushIn flushIn) throws IOException {
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
        public LuceneDocumentInput newDocumentInput() {
            return new LuceneDocumentInput(new ParseContext.Document(), writer);
        }
    }
}
