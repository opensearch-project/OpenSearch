/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.fe.ppl.action;

import org.opensearch.common.collect.Tuple;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.exception.CalciteUnsupportedException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * REST handler that accepts PPL queries via HTTP and dispatches to
 * {@link TransportUnifiedPPLAction}. Parses JSON body with a {@code query}
 * field, validates the query text, and converts the response to JSON with
 * {@code columns}, {@code datarows}, {@code total}, {@code size}, and
 * {@code status} fields.
 *
 * <p>Route: {@code POST /_plugins/_query_engine/_unified/ppl}
 */
public class RestUnifiedPPLAction extends BaseRestHandler {

    public static final String ROUTE_PATH = "/_plugins/_query_engine/_unified/ppl";
    public static final int DEFAULT_MAX_QUERY_LENGTH = 10_000;

    @Override
    public String getName() {
        return "unified_ppl_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, ROUTE_PATH));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // Parse JSON body and extract "query" field
        Tuple<? extends MediaType, Map<String, Object>> bodyTuple = XContentHelper.convertToMap(
            request.content(),
            false,
            request.getMediaType().xContent().mediaType()
        );
        Map<String, Object> body = bodyTuple.v2();

        Object queryObj = body.get("query");
        if (queryObj == null) {
            throw new IllegalArgumentException("Request body is missing the required [query] field");
        }

        String query = queryObj.toString();
        if (query.isEmpty()) {
            throw new IllegalArgumentException("The [query] field must not be empty");
        }

        if (query.length() > DEFAULT_MAX_QUERY_LENGTH) {
            throw new IllegalArgumentException(
                "The [query] field exceeds the maximum allowed length of " + DEFAULT_MAX_QUERY_LENGTH + " characters"
            );
        }

        UnifiedPPLRequest pplRequest = new UnifiedPPLRequest(query);

        return channel -> client.execute(UnifiedPPLExecuteAction.INSTANCE, pplRequest, new ActionListener<>() {
            @Override
            public void onResponse(UnifiedPPLResponse response) {
                try (XContentBuilder builder = channel.newBuilder()) {
                    builder.startObject();
                    // columns
                    builder.startArray("columns");
                    for (String col : response.getColumns()) {
                        builder.value(col);
                    }
                    builder.endArray();
                    // datarows
                    builder.startArray("datarows");
                    for (Object[] row : response.getRows()) {
                        builder.startArray();
                        for (Object val : row) {
                            builder.value(val);
                        }
                        builder.endArray();
                    }
                    builder.endArray();
                    // total, size, status
                    builder.field("total", response.getRows().size());
                    builder.field("size", response.getRows().size());
                    builder.field("status", RestStatus.OK.getStatus());
                    builder.endObject();
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                } catch (IOException e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    RestStatus status = mapExceptionToStatus(e);
                    channel.sendResponse(new BytesRestResponse(channel, status, e));
                } catch (IOException ioException) {
                    throw new RuntimeException("Failed to send error response", ioException);
                }
            }
        });
    }

    /**
     * Maps exception types to appropriate HTTP status codes.
     *
     * <ul>
     *   <li>{@link SyntaxCheckException} → 400 (malformed PPL)</li>
     *   <li>{@link SemanticCheckException} → 400 (unresolved index/field)</li>
     *   <li>{@link CalciteUnsupportedException} → 400 (unsupported PPL construct)</li>
     *   <li>{@link SQLException} → 500 (JDBC execution failure)</li>
     *   <li>All other exceptions → 500 (compilation or engine failures)</li>
     * </ul>
     */
    static RestStatus mapExceptionToStatus(Exception e) {
        Throwable cause = e;
        // Unwrap RuntimeException wrappers to find the root cause
        if (e instanceof RuntimeException && e.getCause() != null) {
            cause = e.getCause();
        }
        if (cause instanceof SyntaxCheckException) {
            return RestStatus.BAD_REQUEST;
        }
        if (cause instanceof SemanticCheckException) {
            return RestStatus.BAD_REQUEST;
        }
        if (cause instanceof CalciteUnsupportedException) {
            return RestStatus.BAD_REQUEST;
        }
        if (cause instanceof SQLException) {
            return RestStatus.INTERNAL_SERVER_ERROR;
        }
        return RestStatus.INTERNAL_SERVER_ERROR;
    }
}
