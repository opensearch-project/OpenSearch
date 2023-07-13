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

package org.opensearch.action.admin.indices.shrink;

import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;

/**
 * A response for a resize index action, either shrink or split index.
 *
 * @opensearch.internal
 */
public final class ResizeResponse extends CreateIndexResponse {

    private static final ConstructingObjectParser<ResizeResponse, Void> PARSER = new ConstructingObjectParser<>(
        "resize_index",
        true,
        args -> new ResizeResponse((boolean) args[0], (boolean) args[1], (String) args[2])
    );

    static {
        declareFields(PARSER);
    }

    ResizeResponse(StreamInput in) throws IOException {
        super(in);
    }

    public ResizeResponse(boolean acknowledged, boolean shardsAcknowledged, String index) {
        super(acknowledged, shardsAcknowledged, index);
    }

    public static ResizeResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(getClass().getSimpleName()).append("[");
        builder.append("acknowledged=").append(isAcknowledged()).append(',');
        builder.append("shards_acknowledged=").append(isShardsAcknowledged()).append(',');
        builder.append("index=").append(index());
        return builder.append(']').toString();
    }
}
