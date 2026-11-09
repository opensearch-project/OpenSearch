/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.store.Directory;

import java.io.IOException;

/**
 * Base IndexWriter factory.
 *
 * @opensearch.api
 */
public interface IndexWriterFactory {
    IndexWriter createWriter(Directory directory, IndexWriterConfig config) throws IOException;

    IndexWriter createWriter(
        Directory directory,
        MergeScheduler mergeScheduler,
        Boolean commitOnClose,
        IndexWriterConfig.OpenMode openMode,
        CombinedDeletionPolicy deletionPolicy,
        SoftDeletesPolicy softDeletesPolicy,
        EngineConfig engineConfig,
        Logger logger,
        String associatedCriteria
    ) throws IOException;
}
