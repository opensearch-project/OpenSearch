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
import org.opensearch.analytics.backend.EngineResultStream;

/**
 * DataFusion EngineBridge implementation (sandbox stub).
 *
 * @deprecated This sandbox stub is superseded by
 * {@code org.opensearch.datafusion.DataFusionBridge} in the engine-datafusion plugin,
 * which provides full native execution via JNI and returns a typed
 * {@code DataFusionResultStream}. This class will be removed once the
 * sandbox analytics-backend-datafusion module is retired.
 */
@Deprecated
public class DataFusionBridge implements EngineBridge<byte[], EngineResultStream, RelNode> {

    /** Creates a new DataFusion bridge. */
    public DataFusionBridge() {}

    /**
     * Convert calcite fragment to an executable native fragment.
     * Ex - substrait for Datafusion
     *
     * @param fragment the logical plan subtree to serialise
     * @return substrait bytes
     * @deprecated Use {@code org.opensearch.datafusion.DataFusionBridge#convertFragment} instead.
     */
    @Deprecated
    @Override
    public byte[] convertFragment(RelNode fragment) {
        return new byte[0];
    }

    /**
     * Execute query fragment
     *
     * @param fragment the serialised plan produced by {@link #convertFragment}
     * @return null (stub — no native execution in sandbox)
     * @deprecated Use {@code org.opensearch.datafusion.DataFusionBridge#execute} instead.
     */
    @Deprecated
    @Override
    public EngineResultStream execute(byte[] fragment) {
        return null;
    }
}
