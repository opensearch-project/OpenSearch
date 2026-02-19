/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Result of a write operation.
 *
 * @param success whether the write was successful
 * @param e the exception if the write failed, null otherwise
 * @param version the document version
 * @param term the primary term
 * @param seqNo the sequence number
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public record WriteResult(boolean success, Exception e, long version, long term, long seqNo) {
}
