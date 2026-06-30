/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.action.bulk.BulkShardRequest;
import org.opensearch.action.support.replication.ReplicatedWriteRequest;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.collect.Tuple;
import org.opensearch.core.common.Strings;
import org.opensearch.http.HttpRequest;
import org.opensearch.rest.RestRequest;
import org.opensearch.tasks.Task;
import org.opensearch.telemetry.tracing.attributes.Attributes;
import org.opensearch.transport.TcpChannel;
import org.opensearch.transport.Transport;

import java.util.Arrays;
import java.util.List;

/**
 * Utility class, helps in creating the {@link SpanCreationContext} for span.
 *
 * @opensearch.internal
 */
@InternalApi
public final class SpanBuilder {

    private static final List<String> HEADERS_TO_BE_ADDED_AS_ATTRIBUTES = Arrays.asList(AttributeNames.TRACE);
    /**
     * Attribute name Separator
     */
    private static final String SEPARATOR = " ";

    /**
     * Constructor
     */
    private SpanBuilder() {

    }

    /**
     * Creates {@link SpanCreationContext} from the {@link HttpRequest}
     * @param request Http request.
     * @return context.
     */
    public static SpanCreationContext from(HttpRequest request) {
        return SpanCreationContext.server().name(createSpanName(request)).attributes(buildSpanAttributes(request));
    }

    /**
     * Creates {@link SpanCreationContext} from the {@link RestRequest}
     * @param request Rest request
     * @return context
     */
    public static SpanCreationContext from(RestRequest request) {
        return SpanCreationContext.client().name(createSpanName(request)).attributes(buildSpanAttributes(request));
    }

    /**
     * Creates {@link SpanCreationContext} from Transport action and connection details.
     * @param action action.
     * @param connection transport connection.
     * @return context
     */
    public static SpanCreationContext from(String action, Transport.Connection connection) {
        return SpanCreationContext.server().name(createSpanName(action, connection)).attributes(buildSpanAttributes(action, connection));
    }

    public static SpanCreationContext from(String spanName, String nodeId, ReplicatedWriteRequest request) {
        return SpanCreationContext.server().name(spanName).attributes(buildSpanAttributes(nodeId, request));
    }

    private static String createSpanName(HttpRequest httpRequest) {
        Tuple<String, String> uriParts = splitUri(httpRequest.uri());
        String path = uriParts.v1();
        return httpRequest.method().name() + SEPARATOR + path;
    }

    private static Attributes buildSpanAttributes(HttpRequest httpRequest) {
        Attributes attributes = Attributes.create()
            .addAttribute(AttributeNames.HTTP_URI, httpRequest.uri())
            .addAttribute(AttributeNames.HTTP_METHOD, httpRequest.method().name())
            .addAttribute(AttributeNames.HTTP_PROTOCOL_VERSION, httpRequest.protocolVersion().name());
        populateHeader(httpRequest, attributes);

        Tuple<String, String> uriParts = splitUri(httpRequest.uri());
        String query = uriParts.v2();
        if (query.isBlank() == false) {
            attributes.addAttribute(AttributeNames.HTTP_REQ_QUERY_PARAMS, query);
        }

        return attributes;
    }

    private static Tuple<String, String> splitUri(String uri) {
        int index = uri.indexOf('?');
        if (index >= 0 && index < uri.length() - 1) {
            String path = uri.substring(0, index);
            String query = uri.substring(index + 1);
            return new Tuple<>(path, query);
        }
        return new Tuple<>(uri, "");
    }

    private static void populateHeader(HttpRequest httpRequest, Attributes attributes) {
        HEADERS_TO_BE_ADDED_AS_ATTRIBUTES.forEach(x -> {
            if (httpRequest.getHeaders() != null
                && httpRequest.getHeaders().get(x) != null
                && (httpRequest.getHeaders().get(x).isEmpty() == false)) {
                attributes.addAttribute(x, Strings.collectionToCommaDelimitedString(httpRequest.getHeaders().get(x)));
            }
        });
    }

    private static String createSpanName(RestRequest restRequest) {
        String spanName = "rest_request";
        if (restRequest != null) {
            try {
                String methodName = restRequest.method().name();
                String rawPath = restRequest.rawPath();
                spanName = methodName + SEPARATOR + rawPath;
            } catch (Exception e) {
                // swallow the exception and keep the default name.
            }
        }
        return spanName;
    }

    private static Attributes buildSpanAttributes(RestRequest restRequest) {
        if (restRequest != null) {
            Attributes attributes = Attributes.create()
                .addAttribute(AttributeNames.REST_REQ_ID, restRequest.getRequestId())
                .addAttribute(AttributeNames.REST_REQ_RAW_PATH, restRequest.rawPath());

            Tuple<String, String> uriParts = splitUri(restRequest.uri());
            String query = uriParts.v2();
            if (query.isBlank() == false) {
                attributes.addAttribute(AttributeNames.HTTP_REQ_QUERY_PARAMS, query);
            }
            return attributes;
        } else {
            return Attributes.EMPTY;
        }
    }

    private static String createSpanName(String action, Transport.Connection connection) {
        return action + SEPARATOR + (connection.getNode() != null ? connection.getNode().getHostAddress() : null);
    }

    private static Attributes buildSpanAttributes(String action, Transport.Connection connection) {
        Attributes attributes = Attributes.create().addAttribute(AttributeNames.TRANSPORT_ACTION, action);
        if (connection != null && connection.getNode() != null) {
            attributes.addAttribute(AttributeNames.TRANSPORT_TARGET_HOST, connection.getNode().getHostAddress());
        }
        return attributes;
    }

    /**
     * Creates {@link SpanCreationContext} from Inbound Handler.
     * @param action action.
     * @param tcpChannel tcp channel.
     * @return context
     */
    public static SpanCreationContext from(String action, TcpChannel tcpChannel) {
        return SpanCreationContext.server().name(createSpanName(action, tcpChannel)).attributes(buildSpanAttributes(action, tcpChannel));
    }

    private static String createSpanName(String action, TcpChannel tcpChannel) {
        return action + SEPARATOR + (tcpChannel.getRemoteAddress() != null
            ? tcpChannel.getRemoteAddress().getHostString()
            : tcpChannel.getLocalAddress().getHostString());
    }

    private static Attributes buildSpanAttributes(String action, TcpChannel tcpChannel) {
        Attributes attributes = Attributes.create().addAttribute(AttributeNames.TRANSPORT_ACTION, action);
        attributes.addAttribute(AttributeNames.TRANSPORT_HOST, tcpChannel.getLocalAddress().getHostString());
        return attributes;
    }

    private static Attributes buildSpanAttributes(String nodeId, ReplicatedWriteRequest request) {
        Attributes attributes = Attributes.create()
            .addAttribute(AttributeNames.NODE_ID, nodeId)
            .addAttribute(AttributeNames.REFRESH_POLICY, request.getRefreshPolicy().getValue());
        if (request.shardId() != null) {
            attributes.addAttribute(AttributeNames.INDEX, request.shardId().getIndexName())
                .addAttribute(AttributeNames.SHARD_ID, request.shardId().getId());
        }
        if (request instanceof BulkShardRequest) {
            attributes.addAttribute(AttributeNames.BULK_REQUEST_ITEMS, ((BulkShardRequest) request).items().length);
        }
        return attributes;
    }

    /**
     * Creates {@link SpanCreationContext} with parent set to specified SpanContext.
     * @param spanName name of span.
     * @param parentSpan target parent span.
     * @return context
     */
    public static SpanCreationContext from(String spanName, SpanContext parentSpan) {
        return SpanCreationContext.server().name(spanName).parent(parentSpan);
    }

    /**
     * Creates {@link SpanCreationContext} with parent set to specified SpanContext.
     * @param task search task.
     * @param actionName action.
     * @return context
     */
    public static SpanCreationContext from(Task task, String actionName) {
        return SpanCreationContext.server().name(createSpanName(task, actionName)).attributes(buildSpanAttributes(task, actionName));
    }

    private static Attributes buildSpanAttributes(Task task, String actionName) {
        Attributes attributes = Attributes.create().addAttribute(AttributeNames.TRANSPORT_ACTION, actionName);
        if (task != null) {
            attributes.addAttribute(AttributeNames.TASK_ID, task.getId());
            if (task.getParentTaskId() != null && task.getParentTaskId().isSet()) {
                attributes.addAttribute(AttributeNames.PARENT_TASK_ID, task.getParentTaskId().getId());
            }
        }
        return attributes;

    }

    private static String createSpanName(Task task, String actionName) {
        if (task != null) {
            return task.getType() + SEPARATOR + task.getAction();
        } else {
            return actionName;
        }
    }
}
