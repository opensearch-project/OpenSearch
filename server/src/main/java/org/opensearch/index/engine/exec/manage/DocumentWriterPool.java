/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.manage;

import org.opensearch.index.engine.exec.composite.CompositeDataFormatWriter;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Supplier;

/**
 * Pooling writers from different underlying engines together to perform a write across all data-formats
 * to ensure documents are coherent.
 *
 * This is currently a basic implementation to showcase how pooling may work. Detailed implementation to follow.
 */
public class DocumentWriterPool {

    private Queue<CompositeDataFormatWriter> writers = new ConcurrentLinkedDeque<>();
    private final Supplier<CompositeDataFormatWriter> writerSupplier;

    public DocumentWriterPool(Supplier<CompositeDataFormatWriter> writerSupplier) {
        this.writerSupplier = writerSupplier;
    }

    // non concurrent
    public CompositeDataFormatWriter fetchWriter() {
        if (writers.isEmpty()) {
            writers.add(writerSupplier.get());
        }
        return writers.poll();
    }

    public void offer(CompositeDataFormatWriter writer) {
        writers.add(writer);
    }

    public List<CompositeDataFormatWriter> freeAll() {
        List<CompositeDataFormatWriter> freeWriters = new ArrayList<>();
        while (!writers.isEmpty()) {
            freeWriters.add(writers.poll());
        }
        return freeWriters;
    }
}
