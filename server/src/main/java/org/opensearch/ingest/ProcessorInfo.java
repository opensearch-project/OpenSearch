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

package org.opensearch.ingest;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Information about an ingest processor
 *
 * @opensearch.internal
 */
public class ProcessorInfo implements Writeable, ToXContentObject, Comparable<ProcessorInfo> {

    private final String type;

    public ProcessorInfo(String type) {
        this.type = type;
    }

    /**
     * Read from a stream.
     */
    public ProcessorInfo(StreamInput input) throws IOException {
        type = input.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.type);
    }

    /**
     * @return The unique processor type
     */
    public String getType() {
        return type;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("type", type);
        builder.endObject();
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ProcessorInfo that = (ProcessorInfo) o;

        return type.equals(that.type);

    }

    @Override
    public int hashCode() {
        return type.hashCode();
    }

    @Override
    public int compareTo(ProcessorInfo o) {
        return type.compareTo(o.type);
    }
}
