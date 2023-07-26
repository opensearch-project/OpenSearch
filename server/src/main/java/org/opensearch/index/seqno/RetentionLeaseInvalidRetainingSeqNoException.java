/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.seqno;

import org.opensearch.OpenSearchException;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Exception thrown when an invalid sequence number is retained
 *
 * @opensearch.internal
 */
public class RetentionLeaseInvalidRetainingSeqNoException extends OpenSearchException {

    RetentionLeaseInvalidRetainingSeqNoException(
        String retentionLeaseId,
        String source,
        long retainingSequenceNumber,
        RetentionLease existingRetentionLease
    ) {
        super(
            "the current retention lease with ["
                + retentionLeaseId
                + "]"
                + " is retaining a higher sequence number ["
                + existingRetentionLease.retainingSequenceNumber()
                + "]"
                + " than the new retaining sequence number ["
                + retainingSequenceNumber
                + "] from ["
                + source
                + "]"
        );
    }

    public RetentionLeaseInvalidRetainingSeqNoException(StreamInput in) throws IOException {
        super(in);
    }
}
