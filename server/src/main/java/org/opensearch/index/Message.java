/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 *  A message ingested from the ingestion source that contains an index operation
 */
@ExperimentalApi
public interface Message<T> {
    T getPayload();
}
