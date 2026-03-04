/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.be.datafusion.jni.ReaderHandle;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;

/**
 * DataFusion reader for JNI operations.
 * <p>
 * Each reader represents a point-in-time snapshot of parquet/arrow files for a shard.
 * Created from a catalog snapshot during refresh; closed when associated catalog snapshot is removed
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DatafusionReader implements Closeable {

    private static final Logger logger = LogManager.getLogger(DatafusionReader.class);
    private final String directoryPath;
    private final ReaderHandle readerHandle;

    /**
     * @param directoryPath shard data directory
     * @param files The file metadata collection
     */
    public DatafusionReader(String directoryPath, Collection<WriterFileSet> files) {
        this.directoryPath = directoryPath;
        String[] fileNames = new String[0];
        if (files != null) {
            fileNames = files.stream().flatMap(writerFileSet -> writerFileSet.files().stream()).toArray(String[]::new);
        }
        readerHandle = new ReaderHandle(directoryPath, fileNames);
    }

    @Override
    public void close() throws IOException {
        readerHandle.close();
        logger.debug("DatafusionReader closed for [{}]", directoryPath);
    }

    public long getReaderPtr() {
        return readerHandle.getPointer();
    }
}
