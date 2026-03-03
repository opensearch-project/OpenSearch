/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.backend;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.rel.RelNode;

import java.util.Iterator;

/**
 * Interface for engine execution.
 * Actual implementations live in engine-specific plugins (e.g., engine-datafusion).
 */
public interface EngineBridge<T> {

    T convertFragment(RelNode fragment);

    Iterator<VectorSchemaRoot> execute(T fragment);
}
