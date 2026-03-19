/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.sql.SqlOperatorTable;
import org.opensearch.analytics.backend.EngineBridge;
import org.opensearch.analytics.spi.AnalyticsBackEndPlugin;
import org.opensearch.index.engine.exec.CatalogSnapshotAwareReaderManager;
import org.opensearch.index.engine.exec.SearchExecEngine;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.SearchAnalyticsBackEndPlugin;
import org.opensearch.plugins.spi.vectorized.DataFormat;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class DataFusionAnalyticsBackend
    implements AnalyticsBackEndPlugin, SearchAnalyticsBackEndPlugin, org.opensearch.datafusion.DataFusionPlugin.ParentAware {

    private org.opensearch.datafusion.DataFusionPlugin parentPlugin;

    public DataFusionAnalyticsBackend() {}

    @Override
    public void setParentPlugin(org.opensearch.datafusion.DataFusionPlugin parent) {
        this.parentPlugin = parent;
    }

    private long getRuntimePtr() {
        return parentPlugin.getDataFusionService().getRuntimePointer();
    }

    @Override
    public String name() {
        return "DataFusion";
    }

    @Override
    public EngineBridge<?, ?, ?> bridge(CatalogSnapshot snapshot) {
        Collection<WriterFileSet> files = snapshot.getSearchableFiles("parquet");
        if (files.isEmpty() || files.stream().allMatch(wfs -> wfs.getFiles().isEmpty())) {
            throw new IllegalStateException("No parquet files available in catalog snapshot");
        }
        String dir = files.stream().findFirst().map(WriterFileSet::getDirectory).orElse("");
        DatafusionReader reader = new DatafusionReader(dir, files);
        return new SandboxDataFusionBridge(getRuntimePtr(), reader);
    }

    @Override
    public SqlOperatorTable operatorTable() {
        return null;
    }

    @Override
    public boolean supportsSearchExecEngine() {
        return true;
    }

    // ---- SearchAnalyticsBackEndPlugin ----

    @Override
    public List<DataFormat> getSupportedFormats() {
        return List.of(DataFormat.PARQUET);
    }

    @Override
    public CatalogSnapshotAwareReaderManager<?> createReaderManager(DataFormat format, ShardPath shardPath) throws IOException {
        return new DatafusionReaderManager(format, shardPath);
    }

    @Override
    public SearchExecEngine<?, ?> createSearchExecEngine(DataFormat format, ShardPath shardPath) throws IOException {
        return new DatafusionSearchExecEngine(getRuntimePtr());
    }
}
