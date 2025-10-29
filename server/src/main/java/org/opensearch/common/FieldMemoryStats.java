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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.common;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A reusable class to encode {@code field -&gt; memory size} mappings
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class FieldMemoryStats extends FieldStats {

    public FieldMemoryStats(Map<String, Long> stats) {
        super(stats);
    }

    public FieldMemoryStats(StreamInput input) throws IOException {
        super(input);
    }

    @Override
    public FieldMemoryStats copy() {
        return new FieldMemoryStats(new HashMap<>(stats));
    }

    @Override
    public void toXContent(XContentBuilder builder, String key, String rawKey, String readableKey) throws IOException {
        builder.startObject(key);
        for (final var entry : stats.entrySet()) {
            builder.startObject(entry.getKey());
            builder.humanReadableField(rawKey, readableKey, new ByteSizeValue(entry.getValue()));
            builder.endObject();
        }
        builder.endObject();
    }

    // TODO: prob make equals() check class equality exactly
    @Override
    public void toXContentField(XContentBuilder builder, String field, String rawKey, String readableKey) throws IOException {
        Objects.requireNonNull(rawKey);
        Objects.requireNonNull(readableKey);
        Objects.requireNonNull(field);
        builder.humanReadableField(rawKey, readableKey, new ByteSizeValue(get(field)));
    }
}
