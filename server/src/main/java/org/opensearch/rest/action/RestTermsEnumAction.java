/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action;

import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.termsenum.TermsEnumAction;
import org.opensearch.action.termsenum.TermsEnumRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * REST handler for {@code GET|POST /{index}/_terms_enum}, which lists the distinct terms of one field.
 *
 * @opensearch.internal
 */
public class RestTermsEnumAction extends BaseRestHandler {

    static final ObjectParser<TermsEnumRequest, Void> PARSER = new ObjectParser<>("terms_enum");

    static {
        PARSER.declareString(TermsEnumRequest::field, new ParseField("field"));
        PARSER.declareString(TermsEnumRequest::string, new ParseField("string"));
        PARSER.declareInt(TermsEnumRequest::size, new ParseField("size"));
        PARSER.declareBoolean(TermsEnumRequest::caseInsensitive, new ParseField("case_insensitive"));
        PARSER.declareString((r, v) -> r.timeout(TimeValue.parseTimeValue(v, "timeout")), new ParseField("timeout"));
        PARSER.declareObject(
            TermsEnumRequest::indexFilter,
            (p, c) -> AbstractQueryBuilder.parseInnerQueryBuilder(p),
            new ParseField("index_filter")
        );
        PARSER.declareString(TermsEnumRequest::searchAfter, new ParseField("search_after"));
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/{index}/_terms_enum"), new Route(POST, "/{index}/_terms_enum"));
    }

    @Override
    public String getName() {
        return "terms_enum_action";
    }

    /**
     * Builds a request from the path, the query parameters and the optional body.
     *
     * @param request the REST request
     * @return the parsed request
     * @throws IOException if the body cannot be read
     */
    static TermsEnumRequest parseRequest(RestRequest request) throws IOException {
        TermsEnumRequest r = new TermsEnumRequest(Strings.splitStringByCommaToArray(request.param("index")));
        r.indicesOptions(IndicesOptions.fromRequest(request, r.indicesOptions()));
        request.withContentOrSourceParamParserOrNull(p -> {
            if (p != null) {
                PARSER.parse(p, r, null);
            }
        });
        return r;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        TermsEnumRequest r = parseRequest(request);
        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).execute(
            TermsEnumAction.INSTANCE,
            r,
            new RestToXContentListener<>(channel)
        );
    }
}
