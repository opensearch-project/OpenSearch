/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.common;

import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Utility class that mirrors {@link org.opensearch.core.xcontent.ObjectParser} functionality for Protocol Buffer parsing.
 *
 * <p>This class provides declarative field parsing methods that explicitly mirror REST's ObjectParser pattern,
 * making it easy to compare gRPC and REST implementations side-by-side.
 *
 * @see org.opensearch.core.xcontent.ObjectParser
 */
public class ObjectParserProtoUtils {

    private ObjectParserProtoUtils() {
        // Utility class
    }

    /**
     * Declares a field, mirroring {@link org.opensearch.core.xcontent.ObjectParser#declareField(BiConsumer, org.opensearch.core.xcontent.ObjectParser.ContextParser, ParseField, org.opensearch.core.xcontent.ObjectParser.ValueType)}.
     *
     * <p>This method dispatches to the consumer when value is not null, optionally transforming
     * the value via the parser function first. Validation happens in the consumer, just like REST.
     *
     * @param builder The builder to set the field on
     * @param consumer The consumer to set the field value (e.g., Builder::field, Builder::missing)
     * @param value The proto value (if null, the field is not set)
     * @param parser Function to transform proto value to builder value (use Function.identity() for no transform)
     * @param fieldName The field name for error context
     * @param <T> The builder type
     * @param <P> The proto value type
     * @param <V> The builder value type
     * @throws IllegalArgumentException if parsing/transformation fails
     */
    public static <T, P, V> void declareField(
        T builder,
        BiConsumer<T, V> consumer,
        P value,
        Function<P, V> parser,
        String fieldName
    ) {
        if (builder == null) {
            throw new IllegalArgumentException("[builder] is required");
        }
        if (consumer == null) {
            throw new IllegalArgumentException("[consumer] is required");
        }
        if (parser == null) {
            throw new IllegalArgumentException("[parser] is required");
        }
        // If value is null, do nothing - matches REST behavior when field not present in JSON
        if (value != null) {
            try {
                V transformedValue = parser.apply(value);
                // Note: Even if transformedValue is null, we still call consumer
                // The consumer is responsible for null validation, just like REST
                consumer.accept(builder, transformedValue);
            } catch (IllegalArgumentException e) {
                throw e;
            } catch (Exception e) {
                throw new IllegalArgumentException(
                    "Failed to parse [" + fieldName + "]",
                    e
                );
            }
        }
    }
}
