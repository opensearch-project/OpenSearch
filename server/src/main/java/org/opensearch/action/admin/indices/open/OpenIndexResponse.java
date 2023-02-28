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

package org.opensearch.action.admin.indices.open;

import org.opensearch.action.support.master.ShardsAcknowledgedResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;

/**
 * A response for a open index action.
 *
 * @opensearch.internal
 */
public class OpenIndexResponse extends ShardsAcknowledgedResponse {

    private static final ConstructingObjectParser<OpenIndexResponse, Void> PARSER = new ConstructingObjectParser<>(
        "open_index",
        true,
        args -> new OpenIndexResponse((boolean) args[0], (boolean) args[1])
    );

    static {
        declareAcknowledgedAndShardsAcknowledgedFields(PARSER);
    }

    public OpenIndexResponse(StreamInput in) throws IOException {
        super(in, true);
    }

    public OpenIndexResponse(boolean acknowledged, boolean shardsAcknowledged) {
        super(acknowledged, shardsAcknowledged);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        writeShardsAcknowledged(out);
    }

    public static OpenIndexResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
