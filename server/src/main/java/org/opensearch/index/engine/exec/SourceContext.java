/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.Closeable;

/**
 * Context for a source provider execution.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface SourceContext extends Closeable {

    Object query();
}
