/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.spi.FragmentConvertor;

import java.nio.charset.StandardCharsets;

/**
 * Records which {@link FragmentConvertor} method was called and what was passed.
 * Shared across DAG / FragmentConversionDriver tests so any test can assert on
 * the convertor surface that {@link FragmentConversionDriver#convertAll} hits.
 */
public class RecordingConvertor implements FragmentConvertor {
    public boolean shardScanCalled;
    public boolean finalAggCalled;
    public String shardScanTableName;
    public RelNode shardScanFragment;
    public RelNode reduceFragment;

    @Override
    public byte[] convertFragment(RelNode fragment) {
        // Distinguish shard-scan vs reduce/final by walking down the leftmost spine to find a
        // TableScan-shaped leaf (annotations are stripped before this is called, so
        // OpenSearchTableScan has been rewritten to LogicalTableScan).
        TableScan scan = RelNodeUtils.findNode(fragment, TableScan.class);
        if (scan != null) {
            this.shardScanCalled = true;
            this.shardScanTableName = scan.getTable().getQualifiedName().getLast();
            this.shardScanFragment = fragment;
            return ("shard:" + this.shardScanTableName).getBytes(StandardCharsets.UTF_8);
        }
        this.finalAggCalled = true;
        this.reduceFragment = fragment;
        return "reduce".getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] attachFragmentOnTop(RelNode fragment, byte[] innerBytes) {
        return ("attach:" + new String(innerBytes, StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] attachPartialAggOnTop(RelNode partialAggFragment, byte[] innerBytes) {
        return ("partialAgg:" + new String(innerBytes, StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] convertSchemaOnlyRead(int stageId, RelDataType schema) {
        return ("schemaOnlyRead:" + stageId).getBytes(StandardCharsets.UTF_8);
    }
}
