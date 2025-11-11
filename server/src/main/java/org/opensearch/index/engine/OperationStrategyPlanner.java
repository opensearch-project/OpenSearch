/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import java.io.IOException;

public interface OperationStrategyPlanner {

    OperationStrategy planOperationAsPrimary(Engine.Operation operation) throws IOException;

    OperationStrategy planOperationAsNonPrimary(Engine.Operation operation) throws IOException;
}
