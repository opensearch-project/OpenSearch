/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.transport.grpc.proto.response.common;

import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.common.breaker.ResponseLimitBreachedException;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.transport.grpc.proto.response.exceptions.CircuitBreakingExceptionProtoUtils;
import org.opensearch.plugin.transport.grpc.proto.response.exceptions.FailedNodeExceptionProtoUtils;
import org.opensearch.plugin.transport.grpc.proto.response.exceptions.ParsingExceptionProtoUtils;
import org.opensearch.plugin.transport.grpc.proto.response.exceptions.ResponseLimitBreachedExceptionProtoUtils;
import org.opensearch.plugin.transport.grpc.proto.response.exceptions.ScriptExceptionProtoUtils;
import org.opensearch.plugin.transport.grpc.proto.response.exceptions.SearchParseExceptionProtoUtils;
import org.opensearch.plugin.transport.grpc.proto.response.exceptions.SearchPhaseExecutionExceptionProtoUtils;
import org.opensearch.plugin.transport.grpc.proto.response.exceptions.TooManyBucketsExceptionProtoUtils;
import org.opensearch.protobufs.ErrorCause;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.protobufs.StringArray;
import org.opensearch.protobufs.StringOrStringArray;
import org.opensearch.script.ScriptException;
import org.opensearch.search.SearchParseException;
import org.opensearch.search.aggregations.MultiBucketConsumerService;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.opensearch.OpenSearchException.OPENSEARCH_PREFIX_KEY;
import static org.opensearch.OpenSearchException.getExceptionName;

/**
 * Utility class for converting Exception objects to Protocol Buffers.
 * This class handles the conversion of OpenSearchException and other Throwable instances
 * to their Protocol Buffer representation.
 */
public class OpenSearchExceptionProtoUtils {

    private OpenSearchExceptionProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts an OpenSearchException to its Protocol Buffer representation.
     * This method is equivalent to the {@link OpenSearchException#toXContent(XContentBuilder, ToXContent.Params)}
     *
     * @param exception The OpenSearchException to convert
     * @return A Protocol Buffer ErrorCause representation
     * @throws IOException if there's an error during conversion
     */
    public static ErrorCause toProto(OpenSearchException exception) throws IOException {
        Throwable ex = ExceptionsHelper.unwrapCause(exception);
        if (ex != exception) {
            return generateThrowableProto(ex);
        } else {
            return innerToProto(
                exception,
                getExceptionName(exception),
                exception.getMessage(),
                exception.getHeaders(),
                exception.getMetadata(),
                exception.getCause()
            );
        }
    }

    /**
     * Static helper method that renders {@link OpenSearchException} or {@link Throwable} instances
     * as Protocol Buffers.
     * <p>
     * This method is usually used when the {@link Throwable} is rendered as a part of another Protocol Buffer object.
     * It is equivalent to the {@link OpenSearchException#generateThrowableXContent(XContentBuilder, ToXContent.Params, Throwable)}
     *
     * @param t The throwable to convert
     * @return A Protocol Buffer ErrorCause representation
     * @throws IOException if there's an error during conversion
     */
    public static ErrorCause generateThrowableProto(Throwable t) throws IOException {
        t = ExceptionsHelper.unwrapCause(t);

        if (t instanceof OpenSearchException) {
            return toProto((OpenSearchException) t);
        } else {
            return innerToProto(t, getExceptionName(t), t.getMessage(), emptyMap(), emptyMap(), t.getCause());
        }
    }

    /**
     * Inner helper method for converting a Throwable to its Protocol Buffer representation.
     * This method is equivalent to the {@link OpenSearchException#innerToXContent(XContentBuilder, ToXContent.Params, Throwable, String, String, Map, Map, Throwable)}.
     *
     * @param throwable The throwable to convert
     * @param type The exception type
     * @param message The exception message
     * @param headers The exception headers
     * @param metadata The exception metadata
     * @param cause The exception cause
     * @return A Protocol Buffer ErrorCause representation
     * @throws IOException if there's an error during conversion
     */
    public static ErrorCause innerToProto(
        Throwable throwable,
        String type,
        String message,
        Map<String, List<String>> headers,
        Map<String, List<String>> metadata,
        Throwable cause
    ) throws IOException {
        ErrorCause.Builder errorCauseBuilder = ErrorCause.newBuilder();

        // Set exception type
        errorCauseBuilder.setType(type);

        // Set exception message (reason)
        if (message != null) {
            errorCauseBuilder.setReason(message);
        }

        // Add custom metadata fields propogated by the child classes of OpenSearchException
        for (Map.Entry<String, List<String>> entry : metadata.entrySet()) {
            Map.Entry<String, ObjectMap.Value> protoEntry = headerToValueProto(
                entry.getKey().substring(OPENSEARCH_PREFIX_KEY.length()),
                entry.getValue()
            );
            errorCauseBuilder.putMetadata(protoEntry.getKey(), protoEntry.getValue());
        }

        // Add metadata if the throwable is an OpenSearchException
        if (throwable instanceof OpenSearchException) {
            OpenSearchException exception = (OpenSearchException) throwable;
            Map<String, ObjectMap.Value> moreMetadata = metadataToProto(exception);
            for (Map.Entry<String, ObjectMap.Value> entry : moreMetadata.entrySet()) {
                errorCauseBuilder.putMetadata(entry.getKey(), entry.getValue());
            }
        }

        if (cause != null) {
            errorCauseBuilder.setCausedBy(generateThrowableProto(cause));
        }

        if (headers.isEmpty() == false) {
            for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
                Map.Entry<String, StringOrStringArray> protoEntry = headerToProto(entry.getKey(), entry.getValue());
                errorCauseBuilder.putHeader(protoEntry.getKey(), protoEntry.getValue());
            }
        }

        // Add stack trace
        errorCauseBuilder.setStackTrace(ExceptionsHelper.stackTrace(throwable));

        // Add suppressed exceptions
        Throwable[] allSuppressed = throwable.getSuppressed();
        if (allSuppressed.length > 0) {
            for (Throwable suppressed : allSuppressed) {
                errorCauseBuilder.addSuppressed(generateThrowableProto(suppressed));
            }
        }

        return errorCauseBuilder.build();
    }

    /**
     * Converts a single entry of a {@code Map<String, List<String>>} into a protobuf {@code <String, StringOrStringArray> }
     * Similar to {@link OpenSearchException#headerToXContent(XContentBuilder, String, List)}
     *
     * @param key The key of the header entry
     * @param values The list of values for the header entry
     * @return A map entry containing the key and its corresponding StringOrStringArray value, or null if values is null or empty
     * @throws IOException if there's an error during conversion
     */
    public static Map.Entry<String, StringOrStringArray> headerToProto(String key, List<String> values) throws IOException {
        if (values != null && values.isEmpty() == false) {
            if (values.size() == 1) {
                return new AbstractMap.SimpleEntry<String, StringOrStringArray>(
                    key,
                    StringOrStringArray.newBuilder().setStringValue(values.get(0)).build()
                );
            } else {
                StringArray.Builder stringArrayBuilder = StringArray.newBuilder();
                for (String val : values) {
                    stringArrayBuilder.addStringArray(val);
                }
                StringOrStringArray stringOrStringArray = StringOrStringArray.newBuilder()
                    .setStringArray(stringArrayBuilder.build())
                    .build();

                return new AbstractMap.SimpleEntry<String, StringOrStringArray>(key, stringOrStringArray);
            }
        }
        return null;
    }

    /**
     * Similar to {@link OpenSearchExceptionProtoUtils#headerToProto(String, List)},
     * but returns a {@code Map<String, ObjectMap.Value>} instead.
     *
     * @param key The key of the header entry
     * @param values The list of values for the header entry
     * @return A map entry containing the key and its corresponding ObjectMap.Value, or null if values is null or empty
     * @throws IOException if there's an error during conversion
     */
    public static Map.Entry<String, ObjectMap.Value> headerToValueProto(String key, List<String> values) throws IOException {
        if (values != null && values.isEmpty() == false) {
            if (values.size() == 1) {
                return new AbstractMap.SimpleEntry<String, ObjectMap.Value>(
                    key,
                    ObjectMap.Value.newBuilder().setString(values.get(0)).build()
                );
            } else {
                ObjectMap.ListValue.Builder listValueBuilder = ObjectMap.ListValue.newBuilder();
                for (String val : values) {
                    listValueBuilder.addValue(ObjectMap.Value.newBuilder().setString(val).build());
                }
                return new AbstractMap.SimpleEntry<String, ObjectMap.Value>(
                    key,
                    ObjectMap.Value.newBuilder().setListValue(listValueBuilder).build()
                );
            }
        }
        return null;
    }

    /**
     * This method is similar to {@link OpenSearchException#metadataToXContent(XContentBuilder, ToXContent.Params)}
     * This method is overridden by various exception classes, which are hardcoded here.
     *
     * @param exception The OpenSearchException to convert metadata from
     * @return A map containing the exception's metadata as ObjectMap.Value objects
     */
    public static Map<String, ObjectMap.Value> metadataToProto(OpenSearchException exception) {
        if (exception instanceof CircuitBreakingException) {
            return CircuitBreakingExceptionProtoUtils.metadataToProto((CircuitBreakingException) exception);
        } else if (exception instanceof FailedNodeException) {
            return FailedNodeExceptionProtoUtils.metadataToProto((FailedNodeException) exception);
        } else if (exception instanceof ParsingException) {
            return ParsingExceptionProtoUtils.metadataToProto((ParsingException) exception);
        } else if (exception instanceof ResponseLimitBreachedException) {
            return ResponseLimitBreachedExceptionProtoUtils.metadataToProto((ResponseLimitBreachedException) exception);
        } else if (exception instanceof ScriptException) {
            return ScriptExceptionProtoUtils.metadataToProto((ScriptException) exception);
        } else if (exception instanceof SearchParseException) {
            return SearchParseExceptionProtoUtils.metadataToProto((SearchParseException) exception);
        } else if (exception instanceof SearchPhaseExecutionException) {
            return SearchPhaseExecutionExceptionProtoUtils.metadataToProto((SearchPhaseExecutionException) exception);
        } else if (exception instanceof MultiBucketConsumerService.TooManyBucketsException) {
            return TooManyBucketsExceptionProtoUtils.metadataToProto((MultiBucketConsumerService.TooManyBucketsException) exception);
        } else {
            return new HashMap<>();
        }
    }
}
