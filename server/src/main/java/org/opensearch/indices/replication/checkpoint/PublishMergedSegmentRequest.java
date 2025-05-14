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

package org.opensearch.indices.replication.checkpoint;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

/**
 * Request object for publish merged segment
 *
 * @opensearch.internal
 */
public class PublishMergedSegmentRequest extends ActionRequest {
    private final ReplicationSegmentCheckpoint mergedSegment;

    public PublishMergedSegmentRequest(ReplicationSegmentCheckpoint mergedSegment) {
        this.mergedSegment = mergedSegment;
    }

    public PublishMergedSegmentRequest(StreamInput in) throws IOException {
        super(in);
        this.mergedSegment = new ReplicationSegmentCheckpoint(in);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        mergedSegment.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PublishMergedSegmentRequest that)) return false;
        return Objects.equals(mergedSegment, that.mergedSegment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mergedSegment);
    }

    @Override
    public String toString() {
        return "PublishMergedSegmentRequest{" + "mergedSegment=" + mergedSegment + '}';
    }

    public ReplicationSegmentCheckpoint getMergedSegment() {
        return mergedSegment;
    }
}
