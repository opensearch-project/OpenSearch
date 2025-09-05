/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.composite;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.DocumentInput;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.FlushIn;
import org.opensearch.index.engine.exec.WriteResult;
import org.opensearch.index.engine.exec.Writer;
import org.opensearch.index.mapper.MappedFieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class CompositeDataFormatWriter implements Writer<CompositeDataFormatWriter.CompositeDocumentInput> {

    List<Writer<? extends DocumentInput>> writers = new ArrayList<>();
    Runnable postWrite;

    public CompositeDataFormatWriter(CompositeIndexingExecutionEngine engine) {
        engine.delegates.forEach(delegate -> {
            try {
                writers.add(delegate.createWriter());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        this.postWrite = () -> engine.pool.offer(this);
    }

    @Override
    public WriteResult addDoc(CompositeDocumentInput d) throws IOException {
        return d.addToWriter();
    }

    @Override
    public FileMetadata flush(FlushIn flushIn) throws IOException {
        FileMetadata metadata = null;
        for  (Writer<? extends DocumentInput> writer : writers) {
            metadata = writer.flush(flushIn);
        }
        return metadata; // todo: model meta in a way that it can handle multiple writers.
    }

    @Override
    public void sync() throws IOException {

    }

    @Override
    public void close() {

    }

    @Override
    public Optional<FileMetadata> getMetadata() {
        return Optional.empty();
    }

    @Override
    public CompositeDocumentInput newDocumentInput() {
        List<DocumentInput<?>> documentInputs = new ArrayList<>();
        return new CompositeDocumentInput(writers.stream().map(Writer::newDocumentInput).collect(Collectors.toList()), this, postWrite);
    }

    @ExperimentalApi
    public static class CompositeDocumentInput implements DocumentInput<List<? extends DocumentInput<?>>> {
        List<? extends DocumentInput<?>> inputs;
        CompositeDataFormatWriter writer;
        Runnable onClose;

        public CompositeDocumentInput(List<? extends DocumentInput<?>> inputs, CompositeDataFormatWriter writer, Runnable onClose) {
            this.inputs = inputs;
            this.writer = writer;
            this.onClose = onClose;
        }

        @Override
        public void addField(MappedFieldType fieldType, Object value) {
            for (DocumentInput<?> input : inputs) {
                input.addField(fieldType, value);
            }
        }

        @Override
        public List<? extends DocumentInput<?>> getFinalInput() {
            return null;
        }

        @Override
        public WriteResult addToWriter() throws IOException {
            WriteResult writeResult = null;
            for (DocumentInput<?> input : inputs) {
                writeResult = input.addToWriter();
            }
            return writeResult;
        }

        @Override
        public void close() throws Exception {
            onClose.run();
        }
    }
}
