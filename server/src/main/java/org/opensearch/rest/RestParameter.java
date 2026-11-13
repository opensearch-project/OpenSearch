/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.rest;

import org.opensearch.common.Booleans;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.unit.ByteSizeValue;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import static org.opensearch.common.unit.TimeValue.parseTimeValue;
import static org.opensearch.core.common.unit.ByteSizeValue.parseBytesSizeValue;

/**
 * A declared REST parameter. Mirrors {@link org.opensearch.common.settings.Setting}: the handler
 * declares a parameter once, and {@link RestRequest#param(RestParameter)} consumes and parses it.
 * <p>
 * An optional path segment is expressed by registering another route. A path parameter absent from
 * some of the handler's routes must be declared without {@link Property#Required}. Query parameters
 * without {@link Property#Required} are optional and return {@code null} when absent. A handler can
 * pass an action request's default to {@link RestRequest#param(RestParameter, Object)}.
 * <pre>{@code
 * static final RestParameter<String> INDEX = RestParameter.simpleString("index", Property.Required, Property.Path);
 * static final RestParameter<Boolean> LOCAL = RestParameter.boolParam("local");
 * static final RestParameter<TimeValue> TIMEOUT = RestParameter.timeValue("timeout");
 *
 * String index = request.param(INDEX);
 * boolean local = request.param(LOCAL, actionRequest.local());
 * TimeValue timeout = request.param(TIMEOUT, actionRequest.timeout());
 * }</pre>
 * {@link BaseRestHandler} validates all required declarations before it invokes
 * {@link BaseRestHandler#prepareRequest(RestRequest, org.opensearch.transport.client.node.NodeClient)}.
 * Handlers expose their declarations from {@link RestHandler#parameters()} so a coverage check can
 * diff them against the API specification without executing a request.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class RestParameter<T> {

    /**
     * How a parameter participates in the REST contract.
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    public enum Property {
        /**
         * The parameter must be present on every route of this handler.
         */
        Required,

        /**
         * The parameter is a path parameter. Absence means the parameter is a query parameter.
         * Optionality of a path segment is expressed by registering more than one route.
         */
        Path,

        /**
         * The parameter is deprecated. The handler still owns consumption and the deprecation log.
         */
        Deprecated
    }

    private final String key;
    private final Function<String, T> parser;
    private final Set<Property> properties;

    private RestParameter(String key, Function<String, T> parser, Property... properties) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("parameter key must not be empty");
        }
        this.key = key;
        this.parser = Objects.requireNonNull(parser, "parameter parser must not be null");
        EnumSet<Property> parsed = EnumSet.noneOf(Property.class);
        for (Property property : Objects.requireNonNull(properties, "parameter properties must not be null")) {
            parsed.add(Objects.requireNonNull(property, "parameter property must not be null"));
        }
        this.properties = Collections.unmodifiableSet(parsed);
    }

    /**
     * A string parameter. Without {@link Property#Required}, a missing value returns {@code null}.
     */
    public static RestParameter<String> simpleString(String key, Property... properties) {
        return new RestParameter<>(key, Function.identity(), properties);
    }

    /**
     * A boolean parameter. An empty value means {@code true}, matching
     * {@link RestRequest#paramAsBoolean(String, boolean)}.
     */
    public static RestParameter<Boolean> boolParam(String key, Property... properties) {
        return new RestParameter<>(key, raw -> parseBoolean(raw, false), properties);
    }

    /**
     * An integer parameter.
     */
    public static RestParameter<Integer> intParam(String key, Property... properties) {
        return new RestParameter<>(key, raw -> parseInt(key, raw), properties);
    }

    /**
     * A long parameter.
     */
    public static RestParameter<Long> longParam(String key, Property... properties) {
        return new RestParameter<>(key, raw -> parseLong(key, raw), properties);
    }

    /**
     * A float parameter.
     */
    public static RestParameter<Float> floatParam(String key, Property... properties) {
        return new RestParameter<>(key, raw -> parseFloat(key, raw), properties);
    }

    /**
     * A time parameter.
     */
    public static RestParameter<TimeValue> timeValue(String key, Property... properties) {
        return new RestParameter<>(key, raw -> parseTimeValue(raw, key), properties);
    }

    /**
     * A byte-size parameter.
     */
    public static RestParameter<ByteSizeValue> byteSize(String key, Property... properties) {
        return new RestParameter<>(key, raw -> parseBytesSizeValue(raw, key), properties);
    }

    /**
     * A comma-separated string array. Without {@link Property#Required}, a missing value returns
     * {@code null}.
     */
    public static RestParameter<String[]> stringArray(String key, Property... properties) {
        return new RestParameter<>(key, Strings::splitStringByCommaToArray, properties);
    }

    /**
     * An enum parameter parsed with {@link Enum#valueOf(Class, String)}.
     */
    public static <E extends Enum<E>> RestParameter<E> enumParam(String key, Class<E> clazz, Property... properties) {
        Objects.requireNonNull(clazz, "enum class must not be null");
        return new RestParameter<>(key, raw -> parseEnum(key, clazz, raw), properties);
    }

    /**
     * A parameter with a caller-supplied parser.
     */
    public static <T> RestParameter<T> param(String key, Function<String, T> parser, Property... properties) {
        return new RestParameter<>(key, parser, properties);
    }

    /**
     * Consumes and parses this parameter. A missing optional value returns {@code null}.
     * Requiredness is validated in bulk by {@link BaseRestHandler}.
     *
     * @throws IllegalArgumentException if the value cannot be parsed
     */
    public T get(RestRequest request) {
        String raw = request.param(key);
        return raw == null ? null : parser.apply(raw);
    }

    public String getKey() {
        return key;
    }

    public boolean isRequired() {
        return hasProperty(Property.Required);
    }

    public boolean isPath() {
        return hasProperty(Property.Path);
    }

    public boolean isDeprecated() {
        return hasProperty(Property.Deprecated);
    }

    public boolean hasProperty(Property property) {
        return properties.contains(property);
    }

    public Set<Property> properties() {
        return properties;
    }

    private static Boolean parseBoolean(String raw, boolean defaultValue) {
        if (raw != null && raw.length() == 0) {
            return true;
        }
        return Booleans.parseBoolean(raw, defaultValue);
    }

    private static Integer parseInt(String key, String raw) {
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("failed to parse int parameter [" + key + "] with value [" + raw + "]", e);
        }
    }

    private static Long parseLong(String key, String raw) {
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("failed to parse long parameter [" + key + "] with value [" + raw + "]", e);
        }
    }

    private static Float parseFloat(String key, String raw) {
        try {
            return Float.parseFloat(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("failed to parse float parameter [" + key + "] with value [" + raw + "]", e);
        }
    }

    private static <E extends Enum<E>> E parseEnum(String key, Class<E> clazz, String raw) {
        try {
            return Enum.valueOf(clazz, raw);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "failed to parse parameter [" + key + "] with value [" + raw + "] as [" + clazz.getSimpleName() + "]",
                e
            );
        }
    }

    @Override
    public String toString() {
        return "RestParameter{key=" + key + ", properties=" + properties + "}";
    }
}
