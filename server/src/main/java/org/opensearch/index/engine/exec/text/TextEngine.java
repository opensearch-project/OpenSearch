/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.text;

import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.DocumentInput;
import org.opensearch.index.engine.exec.FileInfos;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.FlushIn;
import org.opensearch.index.engine.exec.IndexingExecutionEngine;
import org.opensearch.index.engine.exec.Merger;
import org.opensearch.index.engine.exec.RefreshInput;
import org.opensearch.index.engine.exec.RefreshResult;
import org.opensearch.index.engine.exec.WriteResult;
import org.opensearch.index.engine.exec.Writer;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.merge.MergeResult;
import org.opensearch.index.engine.exec.merge.RowIdMapping;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.shard.ShardPath;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class TextEngine implements IndexingExecutionEngine<TextDF> {

    private final AtomicLong counter = new AtomicLong();
    private final Set<TextWriter> openWriters = new HashSet<>();
    private final List<WriterFileSet> openFiles = new ArrayList<>();

    @Override
    public List<String> supportedFieldTypes() {
        return List.of();
    }

    @Override
    public Writer<? extends DocumentInput<?>> createWriter(long writerGeneration) throws IOException {
        return new TextWriter("text_file" + counter.getAndIncrement(), this, writerGeneration);
    }

    @Override
    public Merger getMerger() {
        return new TextMerger();
    }

    @Override
    public DataFormat getDataFormat() {
        return DataFormat.TEXT;
    }

    @Override
    public void loadWriterFiles(ShardPath shardPath) throws IOException {

    }

    @Override
    public RefreshResult refresh(RefreshInput refreshInput) throws IOException {
        openFiles.addAll(refreshInput.getWriterFiles());
        RefreshResult refreshResult = new RefreshResult();
        refreshResult.add(DataFormat.TEXT, openFiles);
        return refreshResult;
    }

    public static class TextInput implements DocumentInput<String> {

        private final StringBuilder sb = new StringBuilder();
        private final TextWriter writer;

        public TextInput(TextWriter writer) {
            this.writer = writer;
        }

        @Override
        public void addRowIdField(String fieldName, long rowId) {
            sb.append(fieldName).append("=").append(rowId).append(";");
        }

        @Override
        public void addField(MappedFieldType fieldType, Object value) {
            sb.append(fieldType.name()).append("=").append(value).append(";");
        }

        @Override
        public String getFinalInput() {
            return sb.append("\n").toString();
        }

        @Override
        public WriteResult addToWriter() throws IOException {
            return writer.addDoc(this);
        }

        @Override
        public void close() throws Exception {
            //no op
        }
    }

    public static class TextMerger implements Merger {

        @Override
        public MergeResult merge(Collection<FileMetadata> fileMetadataList) {
            // Here we will implementation of logic for merging files and reassign the row-ids
            // and creating the mapping of the old segment+id to new row id.
            //
            // Needed when this data format is configured as primary data format.
            throw new UnsupportedOperationException("merge not supported");
        }

        @Override
        public MergeResult merge(Collection<FileMetadata> fileMetadataList, RowIdMapping rowIdMapping) {
            // Here we will have implementation of the merge logic where we will have the mapping of the old row id to new id
            // and merging the files.
            //
            // Needed when data format is not configured as primary data format.
            throw new UnsupportedOperationException("merge not supported");
        }
    }

    public static class TextWriter implements Writer<TextInput> {

        private final StringBuilder sb = new StringBuilder();
        private final File currentFile;
        private final AtomicBoolean flushed = new AtomicBoolean(false);
        private final Runnable onClose;
        private final long writerGeneration;

        public TextWriter(String currentFile, TextEngine engine, long writerGeneration) throws IOException {
            this.currentFile = new File("/Users/shnkgo/mustang" + currentFile);
            this.currentFile.createNewFile();
            this.writerGeneration = writerGeneration;
            boolean canWrite = this.currentFile.setWritable(true);
            if (!canWrite) {
                throw new IllegalStateException("Cannot write to file [" + currentFile + "]");
            }
            engine.openWriters.add(this);
            onClose = () -> engine.openWriters.remove(this);
        }

        @Override
        public WriteResult addDoc(TextInput d) throws IOException {
            sb.append(d.getFinalInput());
            return new WriteResult(true, null, 1, 1, 1);
        }

        @Override
        public FileInfos flush(FlushIn flushIn) throws IOException {
            try (FileWriter fw = new FileWriter(currentFile)) {
                fw.write(sb.toString());
            }
            flushed.set(true);
            WriterFileSet writerFileSet = WriterFileSet.builder()
                .directory(currentFile.toPath().getParent())
                .writerGeneration(writerGeneration)
                .addFile(currentFile.getName())
                .build();
            return FileInfos.builder().putWriterFileSet(DataFormat.TEXT, writerFileSet).build();
        }

        @Override
        public void sync() throws IOException {
        }

        @Override
        public void close() {
            onClose.run();
        }

        @Override
        public TextInput newDocumentInput() {
            return new TextInput(this);
        }

    }
}
