/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.backend.EngineBridge;

import java.util.Iterator;

/**
 * DataFusion EngineBridge implementation.
 * Uses a byte[] representing serialized plan to execute
 */
public class DataFusionBridge implements EngineBridge<byte[]> {

    @Override
    public byte[] convertFragment(RelNode fragment) {
        return new byte[0];
    }

    @Override
    public Iterator<VectorSchemaRoot> execute(byte[] fragment) {
        return null;
    }
}
