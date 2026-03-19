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
 * @opensearch.experimental
 */
@ExperimentalApi
public sealed interface WriteResult permits WriteResult.Success, WriteResult.Failure {

    record Success(long version, long term, long seqNo) implements WriteResult {}

    record Failure(Exception cause, long version, long term, long seqNo) implements WriteResult {}
}
