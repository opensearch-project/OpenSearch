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
import java.io.IOException;

/**
 * @opensearch.experimental
 */
@ExperimentalApi
public interface IndexFilterContext extends Closeable {

    int segmentCount();

    int segmentMaxDoc(int segmentOrd);
}
