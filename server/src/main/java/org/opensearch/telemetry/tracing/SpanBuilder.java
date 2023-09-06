/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.common.annotation.InternalApi;
import org.opensearch.core.common.Strings;
import org.opensearch.http.HttpRequest;
import org.opensearch.rest.RestRequest;
import org.opensearch.telemetry.tracing.attributes.Attributes;
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
        return new SpanCreationContext(createSpanName(request), buildSpanAttributes(request));
    }

    /**
     * Creates {@link SpanCreationContext} from the {@link RestRequest}
     * @param request Rest request
     * @return context
     */
    public static SpanCreationContext from(RestRequest request) {
        return new SpanCreationContext(createSpanName(request), buildSpanAttributes(request));
    }

    /**
     * Creates {@link SpanCreationContext} from Transport action and connection details.
     * @param action action.
     * @param connection transport connection.
     * @return context
     */
    public static SpanCreationContext from(String action, Transport.Connection connection) {
        return new SpanCreationContext(createSpanName(action, connection), buildSpanAttributes(action, connection));
    }

    private static String createSpanName(HttpRequest httpRequest) {
        return httpRequest.method().name() + SEPARATOR + httpRequest.uri();
    }

    private static Attributes buildSpanAttributes(HttpRequest httpRequest) {
        Attributes attributes = Attributes.create()
            .addAttribute(AttributeNames.HTTP_URI, httpRequest.uri())
            .addAttribute(AttributeNames.HTTP_METHOD, httpRequest.method().name())
            .addAttribute(AttributeNames.HTTP_PROTOCOL_VERSION, httpRequest.protocolVersion().name());
        populateHeader(httpRequest, attributes);
        return attributes;
    }

    private static void populateHeader(HttpRequest httpRequest, Attributes attributes) {
        HEADERS_TO_BE_ADDED_AS_ATTRIBUTES.forEach(x -> {
            if (httpRequest.getHeaders() != null && httpRequest.getHeaders().get(x) != null) {
                attributes.addAttribute(x, Strings.collectionToCommaDelimitedString(httpRequest.getHeaders().get(x)));
            }
        });
    }

    private static String createSpanName(RestRequest restRequest) {
        String spanName = "rest_request";
        if (restRequest != null) {
            try {
                String methodName = restRequest.method().name();
                // path() does the decoding, which may give error
                String path = restRequest.path();
                spanName = methodName + SEPARATOR + path;
            } catch (Exception e) {
                // swallow the exception and keep the default name.
            }
        }
        return spanName;
    }

    private static Attributes buildSpanAttributes(RestRequest restRequest) {
        if (restRequest != null) {
            return Attributes.create()
                .addAttribute(AttributeNames.REST_REQ_ID, restRequest.getRequestId())
                .addAttribute(AttributeNames.REST_REQ_RAW_PATH, restRequest.rawPath());
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

}
