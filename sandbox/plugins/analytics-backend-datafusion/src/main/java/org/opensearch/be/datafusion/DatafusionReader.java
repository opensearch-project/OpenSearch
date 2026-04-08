/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.be.datafusion.jni.NativeBridge;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.Closeable;
import java.util.Collection;

/**
 * Thin wrapper around a native DataFusion reader pointer.
 * Calls {@link NativeBridge} directly — no ref counting, lifecycle
 * is managed by {@link DatafusionReaderManager} via catalog snapshot events.
 */
public class DatafusionReader implements Closeable {

    private final long ptr;

    public DatafusionReader(String directoryPath, Collection<WriterFileSet> files) {
        String[] fileNames = files.stream()
            .flatMap(wfs -> wfs.getFiles().stream())
            .toArray(String[]::new);
        this.ptr = NativeBridge.createDatafusionReader(directoryPath, fileNames);
    }

    public long getPtr() {
        return ptr;
    }

    @Override
    public void close() {
        if (ptr != 0) {
            NativeBridge.closeDatafusionReader(ptr);
        }
    }
}
