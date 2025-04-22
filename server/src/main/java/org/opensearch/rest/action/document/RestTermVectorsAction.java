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

package org.opensearch.rest.action.document;

import org.opensearch.action.termvectors.TermVectorsRequest;
import org.opensearch.client.node.NodeClient;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.VersionType;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestActions;
import org.opensearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * This class parses the json request and translates it into a
 * TermVectorsRequest.
 *
 * @opensearch.api
 */
public class RestTermVectorsAction extends BaseRestHandler {
    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal] " + "Specifying types in term vector requests is deprecated.";

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(
                new Route(GET, "/{index}/_termvectors"),
                new Route(POST, "/{index}/_termvectors"),
                new Route(GET, "/{index}/_termvectors/{id}"),
                new Route(POST, "/{index}/_termvectors/{id}")
            )
        );
    }

    @Override
    public String getName() {
        return "document_term_vectors_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        TermVectorsRequest termVectorsRequest = new TermVectorsRequest(request.param("index"), request.param("id"));
        if (request.hasContentOrSourceParam()) {
            try (XContentParser parser = request.contentOrSourceParamParser()) {
                TermVectorsRequest.parseRequest(termVectorsRequest, parser);
            }
        }
        readURIParameters(termVectorsRequest, request);

        return channel -> client.termVectors(termVectorsRequest, new RestToXContentListener<>(channel));
    }

    public static void readURIParameters(TermVectorsRequest termVectorsRequest, RestRequest request) {
        String fields = request.param("fields");
        addFieldStringsFromParameter(termVectorsRequest, fields);
        termVectorsRequest.offsets(request.paramAsBoolean("offsets", termVectorsRequest.offsets()));
        termVectorsRequest.positions(request.paramAsBoolean("positions", termVectorsRequest.positions()));
        termVectorsRequest.payloads(request.paramAsBoolean("payloads", termVectorsRequest.payloads()));
        termVectorsRequest.routing(request.param("routing"));
        termVectorsRequest.realtime(request.paramAsBoolean("realtime", termVectorsRequest.realtime()));
        termVectorsRequest.version(RestActions.parseVersion(request, termVectorsRequest.version()));
        termVectorsRequest.versionType(VersionType.fromString(request.param("version_type"), termVectorsRequest.versionType()));
        termVectorsRequest.preference(request.param("preference"));
        termVectorsRequest.termStatistics(request.paramAsBoolean("termStatistics", termVectorsRequest.termStatistics()));
        termVectorsRequest.termStatistics(request.paramAsBoolean("term_statistics", termVectorsRequest.termStatistics()));
        termVectorsRequest.fieldStatistics(request.paramAsBoolean("fieldStatistics", termVectorsRequest.fieldStatistics()));
        termVectorsRequest.fieldStatistics(request.paramAsBoolean("field_statistics", termVectorsRequest.fieldStatistics()));
    }

    public static void addFieldStringsFromParameter(TermVectorsRequest termVectorsRequest, String fields) {
        Set<String> selectedFields = termVectorsRequest.selectedFields();
        if (fields != null) {
            String[] paramFieldStrings = Strings.commaDelimitedListToStringArray(fields);
            for (String field : paramFieldStrings) {
                if (selectedFields == null) {
                    selectedFields = new HashSet<>();
                }
                if (!selectedFields.contains(field)) {
                    field = field.replaceAll("\\s", "");
                    selectedFields.add(field);
                }
            }
        }
        if (selectedFields != null) {
            termVectorsRequest.selectedFields(selectedFields.toArray(new String[0]));
        }
    }

}
