/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.common;

import org.opensearch.common.annotation.PublicApi;

/**
 * Represents a state object used to track copying of segments from an external source
 *
 * @opensearch.api
 */
@PublicApi(since = "2.2.0")
public interface ReplicationState {
    ReplicationLuceneIndex getIndex();

    ReplicationTimer getTimer();
}
