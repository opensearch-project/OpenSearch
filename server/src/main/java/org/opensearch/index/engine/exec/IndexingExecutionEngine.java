/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.index.shard.ShardPath;

import java.io.IOException;
import java.util.List;

public interface IndexingExecutionEngine<T extends DataFormat> {

    List<String> supportedFieldTypes();

    Writer<? extends DocumentInput<?>> createWriter(long writerGeneration)
        throws IOException; // A writer responsible for data format vended by this engine.

    Merger getMerger(); // Merger responsible for merging for specific data format

    RefreshResult refresh(RefreshInput refreshInput) throws IOException;

    DataFormat getDataFormat();

    void loadWriterFiles(ShardPath shardPath) throws IOException;
}
