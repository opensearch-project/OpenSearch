/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.csv.engine.exec;

import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.DocumentInput;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.FlushIn;
import org.opensearch.index.engine.exec.IndexingExecutionEngine;
import org.opensearch.index.engine.exec.RefreshInput;
import org.opensearch.index.engine.exec.RefreshResult;
import org.opensearch.index.engine.exec.WriteResult;
import org.opensearch.index.engine.exec.Writer;
import org.opensearch.index.mapper.MappedFieldType;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * CSV indexing execution engine.
 */
public class CsvEngine implements IndexingExecutionEngine<CsvDataFormat> {

    private final AtomicLong counter = new AtomicLong();
    private final Set<CsvWriter> openWriters = new HashSet<>();
    private List<FileMetadata> openFiles = new ArrayList<>();
    static CsvDataFormat CSV = new CsvDataFormat();

    /**
     * Creates a new CSV indexing execution engine.
     */
    public CsvEngine() {
        // Default constructor
    }

    @Override
    public List<String> supportedFieldTypes() {
        return List.of();
    }

    @Override
    public Writer<? extends DocumentInput<?>> createWriter() throws IOException {
        return new CsvWriter("file1.csv" + counter.getAndIncrement(), this);
    }

    @Override
    public DataFormat getDataFormat() {
        return CSV;
    }

    @Override
    public RefreshResult refresh(RefreshInput refreshInput) throws IOException {
        openFiles.addAll(refreshInput.getFiles());
        RefreshResult refreshResult = new RefreshResult();
        refreshResult.add(CSV, openFiles);
        return refreshResult;
    }

    /**
     * CSV document input.
     */
    public static class CsvInput implements DocumentInput<String> {
        private final List<String> values = new ArrayList<>();
        private final CsvWriter writer;

        /**
         * Creates a new CsvInput.
         *
         * @param writer the CSV writer
         */
        public CsvInput(CsvWriter writer) {
            this.writer = writer;
        }

        @Override
        public void addField(MappedFieldType fieldType, Object value) {
            String stringValue = value == null ? "" : value.toString();
            if (stringValue.contains(",") || stringValue.contains("\"") || stringValue.contains("\n")) {
                stringValue = "\"" + stringValue.replace("\"", "\"\"") + "\"";
            }
            values.add(stringValue);
        }

        @Override
        public String getFinalInput() {
            return String.join(",", values) + "\n";
        }

        @Override
        public WriteResult addToWriter() throws IOException {
            return writer.addDoc(this);
        }

        @Override
        public void close() throws Exception {
            // no op
        }
    }

    /**
     * CSV writer implementation.
     */
    public static class CsvWriter implements Writer<CsvInput> {
        private final StringBuilder sb = new StringBuilder();
        private final File currentFile;
        private AtomicBoolean flushed = new AtomicBoolean(false);
        private final Runnable onClose;
        private boolean headerWritten = false;

        /**
         * Creates a new CsvWriter.
         *
         * @param currentFile the file name
         * @param engine the CSV engine
         * @throws IOException if an I/O error occurs
         */
        public CsvWriter(String currentFile, CsvEngine engine) throws IOException {
            this.currentFile = new File("/Users/gbh/" + currentFile);
            this.currentFile.createNewFile();
            boolean canWrite = this.currentFile.setWritable(true);
            if (!canWrite) {
                throw new IllegalStateException("Cannot write to file [" + currentFile + "]");
            }
            engine.openWriters.add(this);
            onClose = () -> engine.openWriters.remove(this);
        }

        @Override
        public WriteResult addDoc(CsvInput d) throws IOException {
            sb.append(d.getFinalInput());
            return new WriteResult(true, null, 1, 1, 1);
        }

        @Override
        public FileMetadata flush(FlushIn flushIn) throws IOException {
            try (FileWriter fw = new FileWriter(currentFile)) {
                fw.write(sb.toString());
            }
            flushed.set(true);
            return new FileMetadata(CSV, currentFile.getName());
        }

        @Override
        public void sync() throws IOException {
            // no op
        }

        @Override
        public void close() {
            onClose.run();
        }

        @Override
        public Optional<FileMetadata> getMetadata() {
            if (flushed.get()) {
                return Optional.of(new FileMetadata(CSV, currentFile.getName()));
            }
            return Optional.empty();
        }

        @Override
        public CsvInput newDocumentInput() {
            return new CsvInput(this);
        }

        /**
         * Writes CSV headers.
         *
         * @param headers the header list
         */
        public void writeHeaders(List<String> headers) {
            if (!headerWritten) {
                String headerLine = String.join(",", headers) + "\n";
                sb.insert(0, headerLine);
                headerWritten = true;
            }
        }
    }
}
