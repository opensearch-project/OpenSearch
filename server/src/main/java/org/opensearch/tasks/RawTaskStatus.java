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

package org.opensearch.tasks;

import org.opensearch.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static org.opensearch.common.xcontent.XContentHelper.convertToMap;

/**
 * Raw, unparsed status from the task results index.
 *
 * @opensearch.internal
 */
public class RawTaskStatus implements Task.Status {
    public static final String NAME = "raw";

    private final BytesReference status;

    public RawTaskStatus(BytesReference status) {
        this.status = requireNonNull(status, "status may not be null");
    }

    /**
     * Read from a stream.
     */
    public RawTaskStatus(StreamInput in) throws IOException {
        status = in.readOptionalBytesReference();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalBytesReference(status);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        try (InputStream stream = status.streamInput()) {
            return builder.rawValue(stream, MediaTypeRegistry.xContentType(status));
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public String toString() {
        return Strings.toString(XContentType.JSON, this);
    }

    /**
     * Convert the from XContent to a Map for easy reading.
     */
    public Map<String, Object> toMap() {
        return convertToMap(status, false).v2();
    }

    // Implements equals and hashcode for testing
    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != RawTaskStatus.class) {
            return false;
        }
        RawTaskStatus other = (RawTaskStatus) obj;
        // Totally not efficient, but ok for testing because it ignores order and spacing differences
        return toMap().equals(other.toMap());
    }

    @Override
    public int hashCode() {
        // Totally not efficient, but ok for testing because consistent with equals
        return toMap().hashCode();
    }
}
