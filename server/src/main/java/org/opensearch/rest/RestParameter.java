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
 * declares the parameter once, and {@link #get(RestRequest)} consumes, parses, and applies the
 * required-or-default contract.
 * <p>
 * Path parameters are required for the route that contains them. An optional path segment is still
 * expressed by registering another route, not by a flag on the parameter. Query parameters are
 * optional when they have a default (static, or supplied per call) and required when marked
 * {@link Property#Required}.
 * <pre>{@code
 * static final RestParameter<String> INDEX = RestParameter.simpleString("index", Property.Required, Property.Path);
 * static final RestParameter<Boolean> LOCAL = RestParameter.boolParam("local", false);
 * static final RestParameter<TimeValue> TIMEOUT = RestParameter.timeValue("timeout");
 *
 * String index = INDEX.get(request);
 * boolean local = LOCAL.get(request);
 * TimeValue timeout = TIMEOUT.get(request, actionRequest.timeout());
 * }</pre>
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
         * The parameter must be present. Mutually exclusive with a static default.
         */
        Required,

        /**
         * The parameter is a path parameter. Absence means the parameter is a query parameter.
         * Optionality of a path segment is expressed by registering more than one route.
         */
        Path,

        /**
         * The parameter is accepted but deprecated. Declaration keeps it on the allow-list; the
         * handler still owns the deprecation log.
         */
        Deprecated
    }

    private final String key;
    private final T defaultValue;
    private final boolean hasDefault;
    private final Function<String, T> parser;
    private final Set<Property> properties;

    private RestParameter(String key, T defaultValue, boolean hasDefault, Function<String, T> parser, Property... properties) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("parameter key must not be empty");
        }
        this.key = key;
        this.defaultValue = defaultValue;
        this.hasDefault = hasDefault;
        this.parser = Objects.requireNonNull(parser, "parameter parser must not be null");
        EnumSet<Property> parsed = EnumSet.noneOf(Property.class);
        for (Property property : Objects.requireNonNull(properties, "parameter properties must not be null")) {
            parsed.add(Objects.requireNonNull(property, "parameter property must not be null"));
        }
        this.properties = Collections.unmodifiableSet(parsed);
        if (isRequired() && hasDefault) {
            throw new IllegalArgumentException("parameter [" + key + "] cannot be required and have a default value");
        }
    }

    /**
     * A required or optional string parameter. Without {@link Property#Required}, a missing value
     * returns {@code null}.
     */
    public static RestParameter<String> simpleString(String key, Property... properties) {
        return new RestParameter<>(key, null, false, Function.identity(), properties);
    }

    /**
     * An optional string parameter with a static default.
     */
    public static RestParameter<String> simpleString(String key, String defaultValue, Property... properties) {
        return new RestParameter<>(key, defaultValue, true, Function.identity(), properties);
    }

    /**
     * An optional boolean parameter. An empty value means {@code true}, matching
     * {@link RestRequest#paramAsBoolean(String, boolean)}.
     */
    public static RestParameter<Boolean> boolParam(String key, boolean defaultValue, Property... properties) {
        return new RestParameter<>(key, defaultValue, true, raw -> parseBoolean(raw, defaultValue), properties);
    }

    /**
     * A required boolean parameter.
     */
    public static RestParameter<Boolean> boolParam(String key, Property... properties) {
        requireRequired(key, properties);
        return new RestParameter<>(key, null, false, raw -> parseBoolean(raw, false), properties);
    }

    /**
     * An optional int parameter.
     */
    public static RestParameter<Integer> intParam(String key, int defaultValue, Property... properties) {
        return new RestParameter<>(key, defaultValue, true, raw -> parseInt(key, raw), properties);
    }

    /**
     * A required int parameter.
     */
    public static RestParameter<Integer> intParam(String key, Property... properties) {
        requireRequired(key, properties);
        return new RestParameter<>(key, null, false, raw -> parseInt(key, raw), properties);
    }

    /**
     * An optional long parameter.
     */
    public static RestParameter<Long> longParam(String key, long defaultValue, Property... properties) {
        return new RestParameter<>(key, defaultValue, true, raw -> parseLong(key, raw), properties);
    }

    /**
     * A required long parameter.
     */
    public static RestParameter<Long> longParam(String key, Property... properties) {
        requireRequired(key, properties);
        return new RestParameter<>(key, null, false, raw -> parseLong(key, raw), properties);
    }

    /**
     * An optional float parameter.
     */
    public static RestParameter<Float> floatParam(String key, float defaultValue, Property... properties) {
        return new RestParameter<>(key, defaultValue, true, raw -> parseFloat(key, raw), properties);
    }

    /**
     * An optional time parameter with no static default. Use {@link #get(RestRequest, Object)} to
     * supply the transport request's own default.
     */
    public static RestParameter<TimeValue> timeValue(String key, Property... properties) {
        return new RestParameter<>(key, null, false, raw -> parseTimeValue(raw, key), properties);
    }

    /**
     * An optional time parameter with a static default.
     */
    public static RestParameter<TimeValue> timeValue(String key, TimeValue defaultValue, Property... properties) {
        return new RestParameter<>(key, defaultValue, true, raw -> parseTimeValue(raw, defaultValue, key), properties);
    }

    /**
     * An optional byte-size parameter with no static default.
     */
    public static RestParameter<ByteSizeValue> byteSize(String key, Property... properties) {
        return new RestParameter<>(key, null, false, raw -> parseBytesSizeValue(raw, key), properties);
    }

    /**
     * An optional byte-size parameter with a static default.
     */
    public static RestParameter<ByteSizeValue> byteSize(String key, ByteSizeValue defaultValue, Property... properties) {
        return new RestParameter<>(key, defaultValue, true, raw -> parseBytesSizeValue(raw, defaultValue, key), properties);
    }

    /**
     * A comma-separated string array. A missing optional value returns {@code defaultValue}.
     */
    public static RestParameter<String[]> stringArray(String key, String[] defaultValue, Property... properties) {
        return new RestParameter<>(key, defaultValue, true, Strings::splitStringByCommaToArray, properties);
    }

    /**
     * A comma-separated string array. Without {@link Property#Required}, a missing value returns
     * an empty array.
     */
    public static RestParameter<String[]> stringArray(String key, Property... properties) {
        if (isRequired(properties)) {
            return new RestParameter<>(key, null, false, Strings::splitStringByCommaToArray, properties);
        }
        return stringArray(key, Strings.EMPTY_ARRAY, properties);
    }

    /**
     * An enum parameter parsed with {@link Enum#valueOf(Class, String)}.
     */
    public static <E extends Enum<E>> RestParameter<E> enumParam(String key, Class<E> clazz, E defaultValue, Property... properties) {
        Objects.requireNonNull(clazz, "enum class must not be null");
        return new RestParameter<>(key, defaultValue, true, raw -> parseEnum(key, clazz, raw), properties);
    }

    /**
     * A required enum parameter.
     */
    public static <E extends Enum<E>> RestParameter<E> enumParam(String key, Class<E> clazz, Property... properties) {
        requireRequired(key, properties);
        Objects.requireNonNull(clazz, "enum class must not be null");
        return new RestParameter<>(key, null, false, raw -> parseEnum(key, clazz, raw), properties);
    }

    /**
     * A parameter with a caller-supplied parser and no static default.
     */
    public static <T> RestParameter<T> param(String key, Function<String, T> parser, Property... properties) {
        return new RestParameter<>(key, null, false, parser, properties);
    }

    /**
     * A parameter with a caller-supplied parser and a static default.
     */
    public static <T> RestParameter<T> param(String key, T defaultValue, Function<String, T> parser, Property... properties) {
        return new RestParameter<>(key, defaultValue, true, parser, properties);
    }

    /**
     * Consumes and parses this parameter.
     *
     * @throws IllegalArgumentException if a required parameter is missing, or the value cannot be parsed
     */
    public T get(RestRequest request) {
        String raw = request.param(key);
        if (raw == null) {
            if (isRequired()) {
                throw missing(request);
            }
            return hasDefault ? defaultValue : null;
        }
        return parser.apply(raw);
    }

    /**
     * Consumes and parses this parameter, using {@code dynamicDefault} when it is absent.
     * The dynamic default replaces a static default for this call. Required parameters reject
     * a fallback: a missing required parameter is a request error, not a default.
     *
     * @throws IllegalStateException if this parameter is required
     * @throws IllegalArgumentException if the value cannot be parsed
     */
    public T get(RestRequest request, T dynamicDefault) {
        if (isRequired()) {
            throw new IllegalStateException("parameter [" + key + "] is required and cannot take a fallback default");
        }
        String raw = request.param(key);
        if (raw == null) {
            return dynamicDefault;
        }
        return parser.apply(raw);
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

    public boolean hasDefault() {
        return hasDefault;
    }

    public T defaultValue() {
        return defaultValue;
    }

    public boolean hasProperty(Property property) {
        return properties.contains(property);
    }

    public Set<Property> properties() {
        return properties;
    }

    private IllegalArgumentException missing(RestRequest request) {
        return new IllegalArgumentException("request [" + request.path() + "] is missing required parameter [" + key + "]");
    }

    private static void requireRequired(String key, Property... properties) {
        if (isRequired(properties) == false) {
            throw new IllegalArgumentException("parameter [" + key + "] has no default and must be Property.Required");
        }
    }

    private static boolean isRequired(Property... properties) {
        if (properties == null) {
            return false;
        }
        for (Property property : properties) {
            if (property == Property.Required) {
                return true;
            }
        }
        return false;
    }

    private static Boolean parseBoolean(String raw, boolean defaultValue) {
        // An empty query value means "turn this on", matching RestRequest#paramAsBoolean.
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RestParameter<?> that = (RestParameter<?>) o;
        return hasDefault == that.hasDefault && key.equals(that.key) && properties.equals(that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, hasDefault, properties);
    }
}
