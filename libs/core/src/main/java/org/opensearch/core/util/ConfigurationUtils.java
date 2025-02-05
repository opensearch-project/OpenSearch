/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.core.util;

import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchParseException;
import org.opensearch.common.annotation.PublicApi;

import java.util.Map;

/**
 * Utility class for parsing configurations.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.0.0")
public final class ConfigurationUtils {

    private ConfigurationUtils() {}

    /**
     * Returns and removes the specified optional property from the specified configuration map.
     * <p>
     * If the property value isn't of type string a {@link OpenSearchParseException} is thrown.
     */
    public static String readOptionalStringProperty(Map<String, Object> configuration, String propertyName) {
        Object value = configuration.get(propertyName);
        return readString(propertyName, value);
    }

    /**
     * Returns and removes the specified property from the specified configuration map.
     * <p>
     * If the property value isn't of type string an {@link OpenSearchParseException} is thrown.
     * If the property is missing an {@link OpenSearchParseException} is thrown
     */
    public static String readStringProperty(Map<String, Object> configuration, String propertyName) {
        return readStringProperty(configuration, propertyName, null);
    }

    /**
     * Returns the specified property from the specified configuration map.
     * <p>
     * If the property value isn't of type string a {@link OpenSearchParseException} is thrown.
     * If the property is missing and no default value has been specified a {@link OpenSearchParseException} is thrown
     */
    public static String readStringProperty(Map<String, Object> configuration, String propertyName, String defaultValue) {
        Object value = configuration.get(propertyName);
        if (value == null && defaultValue != null) {
            return defaultValue;
        } else if (value == null) {
            throw newConfigurationException(propertyName, "required property is missing");
        }
        return readString(propertyName, value);
    }

    public static OpenSearchException newConfigurationException(String propertyName, String reason) {
        String msg;
        if (propertyName == null) {
            msg = reason;
        } else {
            msg = "[" + propertyName + "] " + reason;
        }
        OpenSearchParseException exception = new OpenSearchParseException(msg);
        addMetadataToException(exception, propertyName);
        return exception;
    }

    private static String readString(String propertyName, Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String) {
            return (String) value;
        }
        throw newConfigurationException(propertyName, "property isn't a string, but of type [" + value.getClass().getName() + "]");
    }

    private static void addMetadataToException(OpenSearchException exception, String propertyName) {
        if (propertyName != null) {
            exception.addMetadata("opensearch.property_name", propertyName);
        }
    }

    /**
     * Returns the specified property from the specified configuration map.
     * <p>
     * If the property value isn't of type string or int a {@link OpenSearchParseException} is thrown.
     * If the property is missing and no default value has been specified a {@link OpenSearchParseException} is thrown
     */
    public static String readStringOrIntProperty(Map<String, Object> configuration, String propertyName, String defaultValue) {
        Object value = configuration.get(propertyName);
        if (value == null && defaultValue != null) {
            return defaultValue;
        } else if (value == null) {
            throw newConfigurationException(propertyName, "required property is missing");
        }
        return readStringOrInt(propertyName, value);
    }

    private static String readStringOrInt(String propertyName, Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String) {
            return (String) value;
        } else if (value instanceof Integer) {
            return String.valueOf(value);
        }
        throw newConfigurationException(propertyName, "property isn't a string or int, but of type [" + value.getClass().getName() + "]");
    }

    /**
     * Returns the specified property from the specified configuration map.
     * <p>
     * If the property value isn't of type string or int a {@link OpenSearchParseException} is thrown.
     */
    public static String readOptionalStringOrIntProperty(Map<String, Object> configuration, String propertyName) {
        Object value = configuration.get(propertyName);
        if (value == null) {
            return null;
        }
        return readStringOrInt(propertyName, value);
    }

    public static boolean readBooleanProperty(Map<String, Object> configuration, String propertyName, boolean defaultValue) {
        Object value = configuration.get(propertyName);
        if (value == null) {
            return defaultValue;
        } else {
            return readBoolean(propertyName, value).booleanValue();
        }
    }

    private static Boolean readBoolean(String propertyName, Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Boolean) {
            return (boolean) value;
        }
        throw newConfigurationException(propertyName, "property isn't a boolean, but of type [" + value.getClass().getName() + "]");
    }

    /**
     * Returns the specified property from the specified configuration map.
     * <p>
     * If the property value isn't of type int a {@link OpenSearchParseException} is thrown.
     * If the property is missing an {@link OpenSearchParseException} is thrown
     */
    public static Integer readIntProperty(Map<String, Object> configuration, String propertyName, Integer defaultValue) {
        Object value = configuration.get(propertyName);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.toString());
        } catch (Exception e) {
            throw newConfigurationException(propertyName, "property cannot be converted to an int [" + value + "]");
        }
    }

    /**
     * Returns the specified property from the specified configuration map.
     * <p>
     * If the property value isn't of type int a {@link OpenSearchParseException} is thrown.
     * If the property is missing an {@link OpenSearchParseException} is thrown
     */
    public static Double readDoubleProperty(Map<String, Object> configuration, String propertyName) {
        Object value = configuration.get(propertyName);
        if (value == null) {
            throw newConfigurationException(propertyName, "required property is missing");
        }
        try {
            return Double.parseDouble(value.toString());
        } catch (Exception e) {
            throw newConfigurationException(propertyName, "property cannot be converted to a double [" + value + "]");
        }
    }
}
