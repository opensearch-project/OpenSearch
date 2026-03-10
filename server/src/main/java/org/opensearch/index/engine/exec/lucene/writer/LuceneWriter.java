/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.writer;

import org.apache.lucene.index.IndexWriter;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.EngineRole;
import org.opensearch.index.engine.exec.FileInfos;
import org.opensearch.index.engine.exec.FlushIn;
import org.opensearch.index.engine.exec.WriteResult;
import org.opensearch.index.engine.exec.Writer;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.mapper.ParseContext;

import java.io.IOException;
import java.nio.file.Path;

public class LuceneWriter implements Writer<LuceneDocumentInput> {

    private final IndexWriter writer;
    private final long writerGeneration;
    private final Path directoryPath;
    private final EngineRole engineRole;

    public LuceneWriter(Path directoryPath, IndexWriter writer, long writerGeneration, EngineRole engineRole) {
        this.directoryPath = directoryPath;
        this.writer = writer;
        this.writerGeneration = writerGeneration;
        this.engineRole = engineRole;
    }

    @Override
    public WriteResult addDoc(LuceneDocumentInput documentInput) throws IOException {
        return documentInput.addToWriter();
    }

    @Override
    public FileInfos flush(FlushIn flushIn) throws IOException {
        writer.forceMerge(1);
        WriterFileSet.Builder writerFileSetBuilder =
            WriterFileSet.builder().directory(directoryPath).writerGeneration(writerGeneration).addNumRows(writer.getDocStats().numDocs);
        return FileInfos.builder().putWriterFileSet(DataFormat.LUCENE, writerFileSetBuilder.build()).build();
    }

    @Override
    public void sync() throws IOException {

    }

    @Override
    public void close() throws IOException {
        writer.close();
    }

    @Override
    public LuceneDocumentInput newDocumentInput() {
        return new LuceneDocumentInput(new ParseContext.Document(), writer, engineRole);
    }
}
