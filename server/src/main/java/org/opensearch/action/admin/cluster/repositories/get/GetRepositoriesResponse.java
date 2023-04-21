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

package org.opensearch.action.admin.cluster.repositories.get;

import org.opensearch.action.ActionResponse;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Get repositories response
 *
 * @opensearch.internal
 */
public class GetRepositoriesResponse extends ActionResponse implements ToXContentObject {

    private RepositoriesMetadata repositories;

    GetRepositoriesResponse(RepositoriesMetadata repositories) {
        this.repositories = repositories;
    }

    GetRepositoriesResponse(StreamInput in) throws IOException {
        repositories = new RepositoriesMetadata(in);
    }

    /**
     * List of repositories to return
     *
     * @return list or repositories
     */
    public List<RepositoryMetadata> repositories() {
        return repositories.repositories();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        repositories.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        repositories.toXContent(
            builder,
            new DelegatingMapParams(Collections.singletonMap(RepositoriesMetadata.HIDE_GENERATIONS_PARAM, "true"), params)
        );
        builder.endObject();
        return builder;
    }

    public static GetRepositoriesResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        return new GetRepositoriesResponse(RepositoriesMetadata.fromXContent(parser));
    }
}
