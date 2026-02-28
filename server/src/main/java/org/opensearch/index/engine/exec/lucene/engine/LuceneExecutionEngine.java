/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.engine;

import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.DocumentInput;
import org.opensearch.index.engine.exec.IndexingExecutionEngine;
import org.opensearch.index.engine.exec.Merger;
import org.opensearch.index.engine.exec.RefreshInput;
import org.opensearch.index.engine.exec.RefreshResult;
import org.opensearch.index.engine.exec.Writer;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.lucene.LuceneDataFormat;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class LuceneExecutionEngine implements IndexingExecutionEngine<LuceneDataFormat> {

    public LuceneExecutionEngine(MapperService mapperService, ShardPath shardPath, IndexSettings indexSettings) {

    }

    @Override
    public List<String> supportedFieldTypes() {
        return List.of();
    }

    @Override
    public Writer<? extends DocumentInput<?>> createWriter(long writerGeneration) throws IOException {
        return null;
    }

    @Override
    public Merger getMerger() {
        return null;
    }

    @Override
    public RefreshResult refresh(RefreshInput refreshInput) throws IOException {
        return null;
    }

    @Override
    public DataFormat getDataFormat() {
        return null;
    }

    @Override
    public void loadWriterFiles(CatalogSnapshot catalogSnapshot) throws IOException {

    }

    @Override
    public void deleteFiles(Map<String, Collection<String>> filesToDelete) throws IOException {

    }

    @Override
    public void close() throws IOException {

    }
}
