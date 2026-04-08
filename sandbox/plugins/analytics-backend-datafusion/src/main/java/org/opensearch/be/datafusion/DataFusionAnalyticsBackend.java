/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.opensearch.analytics.backend.EngineBridge;
import org.opensearch.analytics.spi.AnalyticsBackEndPlugin;
import org.opensearch.datafusion.DatafusionEngine;
import org.opensearch.index.engine.exec.CatalogSnapshotAwareReaderManager;
import org.opensearch.index.engine.exec.SearchExecEngine;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CompositeEngine;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.SearchAnalyticsBackEndPlugin;
import org.opensearch.plugins.spi.vectorized.DataFormat;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

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
    public EngineBridge<?, ?, ?> bridge(CompositeEngine engine, CatalogSnapshot snapshot) {
        try {
            return new SandboxDataFusionBridge(getRuntimePtr(), (DatafusionReader) engine.getReader(name(), snapshot));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SqlOperatorTable operatorTable() {
        return new DataFusionOperatorTable();
    }

    @Override
    public Set<Class<? extends RelNode>> supportedOperators() {
        return Set.of(
            org.apache.calcite.rel.logical.LogicalTableScan.class,
            org.apache.calcite.rel.logical.LogicalFilter.class,
            org.apache.calcite.rel.logical.LogicalAggregate.class,
            org.apache.calcite.rel.logical.LogicalProject.class
        );
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
