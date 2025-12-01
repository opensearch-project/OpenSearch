/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.ShardPath;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface IndexingExecutionEngine<T extends DataFormat> extends Closeable {

    List<String> supportedFieldTypes();

    Writer<? extends DocumentInput<?>> createWriter(long writerGeneration)
        throws IOException; // A writer responsible for data format vended by this engine.

    Merger getMerger(); // Merger responsible for merging for specific data format

    RefreshResult refresh(RefreshInput refreshInput) throws IOException;

    DataFormat getDataFormat();

    void loadWriterFiles(CatalogSnapshot catalogSnapshot)
        throws IOException; // Bootstrap hook to make engine aware of previously written files from CatalogSnapshot

    default long getNativeBytesUsed() {
        return 0;
    }

    void deleteFiles(Map<String, Collection<String>> filesToDelete) throws IOException;
}
