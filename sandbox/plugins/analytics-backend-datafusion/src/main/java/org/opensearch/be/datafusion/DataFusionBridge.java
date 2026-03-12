/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.backend.EngineBridge;

/**
 * DataFusion EngineBridge implementation.
 * Uses a byte[] representing serialized plan to execute.
 */
public class DataFusionBridge implements EngineBridge<byte[], Long, RelNode> {
    // S=byte[] (Substrait), H=Long (stream pointer), L=RelNode (logical plan)

    /** Creates a new DataFusion bridge. */
    public DataFusionBridge() {}

    /**
     * Convert calcite fragment to an executable native fragment.
     * Ex - substrait for Datafusion
     *
     * @param fragment the logical plan subtree to serialise
     * @return substrait bytes
     */
    @Override
    public byte[] convertFragment(RelNode fragment) {
        return new byte[0];
    }

    /**
     * Execute query fragment
     *
     * @param fragment the serialised plan produced by {@link #convertFragment}
     * @return RecordBatchStream pointer
     */
    @Override
    public Long execute(byte[] fragment) {
        return 0L;
    }
}
