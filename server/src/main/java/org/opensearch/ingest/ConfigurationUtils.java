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

package org.opensearch.ingest;

import java.io.IOException;
import java.io.InputStream;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchParseException;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptService;
import org.opensearch.script.ScriptType;
import org.opensearch.script.TemplateScript;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.script.Script.DEFAULT_TEMPLATE_LANG;

/**
 * Utility class for ingest processor configurations
 *
 * @opensearch.internal
 */
public final class ConfigurationUtils {

    public static final String TAG_KEY = "tag";
    public static final String DESCRIPTION_KEY = "description";
    public static final String IGNORE_FAILURE_KEY = "ignore_failure";

    private ConfigurationUtils() {}

    /**
     * Returns and removes the specified optional property from the specified configuration map.
     *
     * If the property value isn't of type string a {@link OpenSearchParseException} is thrown.
     */
    public static String readOptionalStringProperty(
        String processorType,
        String processorTag,
        Map<String, Object> configuration,
        String propertyName
    ) {
        Object value = configuration.remove(propertyName);
        return readString(processorType, processorTag, propertyName, value);
    }

    /**
     * Returns and removes the specified property from the specified configuration map.
     *
     * If the property value isn't of type string an {@link OpenSearchParseException} is thrown.
     * If the property is missing an {@link OpenSearchParseException} is thrown
     */
    public static String readStringProperty(
        String processorType,
        String processorTag,
        Map<String, Object> configuration,
        String propertyName
    ) {
        return readStringProperty(processorType, processorTag, configuration, propertyName, null);
    }

    /**
     * Returns and removes the specified property from the specified configuration map.
     *
     * If the property value isn't of type string a {@link OpenSearchParseException} is thrown.
     * If the property is missing and no default value has been specified a {@link OpenSearchParseException} is thrown
     */
    public static String readStringProperty(
        String processorType,
        String processorTag,
        Map<String, Object> configuration,
        String propertyName,
        String defaultValue
    ) {
        Object value = configuration.remove(propertyName);
        if (value == null && defaultValue != null) {
            return defaultValue;
        } else if (value == null) {
            throw newConfigurationException(processorType, processorTag, propertyName, "required property is missing");
        }
        return readString(processorType, processorTag, propertyName, value);
    }

    private static String readString(String processorType, String processorTag, String propertyName, Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String) {
            return (String) value;
        }
        throw newConfigurationException(
            processorType,
            processorTag,
            propertyName,
            "property isn't a string, but of type [" + value.getClass().getName() + "]"
        );
    }

    /**
     * Returns and removes the specified property from the specified configuration map.
     *
     * If the property value isn't of type string or int a {@link OpenSearchParseException} is thrown.
     * If the property is missing and no default value has been specified a {@link OpenSearchParseException} is thrown
     */
    public static String readStringOrIntProperty(
        String processorType,
        String processorTag,
        Map<String, Object> configuration,
        String propertyName,
        String defaultValue
    ) {
        Object value = configuration.remove(propertyName);
        if (value == null && defaultValue != null) {
            return defaultValue;
        } else if (value == null) {
            throw newConfigurationException(processorType, processorTag, propertyName, "required property is missing");
        }
        return readStringOrInt(processorType, processorTag, propertyName, value);
    }

    private static String readStringOrInt(String processorType, String processorTag, String propertyName, Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String) {
            return (String) value;
        } else if (value instanceof Integer) {
            return String.valueOf(value);
        }
        throw newConfigurationException(
            processorType,
            processorTag,
            propertyName,
            "property isn't a string or int, but of type [" + value.getClass().getName() + "]"
        );
    }

    /**
     * Returns and removes the specified property from the specified configuration map.
     *
     * If the property value isn't of type string or int a {@link OpenSearchParseException} is thrown.
     */
    public static String readOptionalStringOrIntProperty(
        String processorType,
        String processorTag,
        Map<String, Object> configuration,
        String propertyName
    ) {
        Object value = configuration.remove(propertyName);
        if (value == null) {
            return null;
        }
        return readStringOrInt(processorType, processorTag, propertyName, value);
    }

    public static boolean readBooleanProperty(
        String processorType,
        String processorTag,
        Map<String, Object> configuration,
        String propertyName,
        boolean defaultValue
    ) {
        Object value = configuration.remove(propertyName);
        if (value == null) {
            return defaultValue;
        } else {
            return readBoolean(processorType, processorTag, propertyName, value).booleanValue();
        }
    }

    private static Boolean readBoolean(String processorType, String processorTag, String propertyName, Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Boolean) {
            return (boolean) value;
        }
        throw newConfigurationException(
            processorType,
            processorTag,
            propertyName,
            "property isn't a boolean, but of type [" + value.getClass().getName() + "]"
        );
    }

    /**
     * Returns and removes the specified property from the specified configuration map.
     *
     * If the property value isn't of type int a {@link OpenSearchParseException} is thrown.
     * If the property is missing an {@link OpenSearchParseException} is thrown
     */
    public static Integer readIntProperty(
        String processorType,
        String processorTag,
        Map<String, Object> configuration,
        String propertyName,
        Integer defaultValue
    ) {
        Object value = configuration.remove(propertyName);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.toString());
        } catch (Exception e) {
            throw newConfigurationException(
                processorType,
                processorTag,
                propertyName,
                "property cannot be converted to an int [" + value.toString() + "]"
            );
        }
    }

    /**
     * Returns and removes the specified property from the specified configuration map.
     *
     * If the property value isn't of type int a {@link OpenSearchParseException} is thrown.
     * If the property is missing an {@link OpenSearchParseException} is thrown
     */
    public static Double readDoubleProperty(
        String processorType,
        String processorTag,
        Map<String, Object> configuration,
        String propertyName
    ) {
        Object value = configuration.remove(propertyName);
        if (value == null) {
            throw newConfigurationException(processorType, processorTag, propertyName, "required property is missing");
        }
        try {
            return Double.parseDouble(value.toString());
        } catch (Exception e) {
            throw newConfigurationException(
                processorType,
                processorTag,
                propertyName,
                "property cannot be converted to a double [" + value.toString() + "]"
            );
        }
    }

    /**
     * Returns and removes the specified property of type list from the specified configuration map.
     *
     * If the property value isn't of type list an {@link OpenSearchParseException} is thrown.
     */
    public static <T> List<T> readOptionalList(
        String processorType,
        String processorTag,
        Map<String, Object> configuration,
        String propertyName
    ) {
        Object value = configuration.remove(propertyName);
        if (value == null) {
            return null;
        }
        return readList(processorType, processorTag, propertyName, value);
    }

    /**
     * Returns and removes the specified property of type list from the specified configuration map.
     *
     * If the property value isn't of type list an {@link OpenSearchParseException} is thrown.
     * If the property is missing an {@link OpenSearchParseException} is thrown
     */
    public static <T> List<T> readList(String processorType, String processorTag, Map<String, Object> configuration, String propertyName) {
        Object value = configuration.remove(propertyName);
        if (value == null) {
            throw newConfigurationException(processorType, processorTag, propertyName, "required property is missing");
        }

        return readList(processorType, processorTag, propertyName, value);
    }

    private static <T> List<T> readList(String processorType, String processorTag, String propertyName, Object value) {
        if (value instanceof List) {
            @SuppressWarnings("unchecked")
            List<T> stringList = (List<T>) value;
            return stringList;
        } else {
            throw newConfigurationException(
                processorType,
                processorTag,
                propertyName,
                "property isn't a list, but of type [" + value.getClass().getName() + "]"
            );
        }
    }

    /**
     * Returns and removes the specified property of type map from the specified configuration map.
     *
     * If the property value isn't of type map an {@link OpenSearchParseException} is thrown.
     * If the property is missing an {@link OpenSearchParseException} is thrown
     */
    public static <T> Map<String, T> readMap(
        String processorType,
        String processorTag,
        Map<String, Object> configuration,
        String propertyName
    ) {
        Object value = configuration.remove(propertyName);
        if (value == null) {
            throw newConfigurationException(processorType, processorTag, propertyName, "required property is missing");
        }

        return readMap(processorType, processorTag, propertyName, value);
    }

    /**
     * Returns and removes the specified property of type map from the specified configuration map.
     *
     * If the property value isn't of type map an {@link OpenSearchParseException} is thrown.
     */
    public static <T> Map<String, T> readOptionalMap(
        String processorType,
        String processorTag,
        Map<String, Object> configuration,
        String propertyName
    ) {
        Object value = configuration.remove(propertyName);
        if (value == null) {
            return null;
        }

        return readMap(processorType, processorTag, propertyName, value);
    }

    private static <T> Map<String, T> readMap(String processorType, String processorTag, String propertyName, Object value) {
        if (value instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, T> map = (Map<String, T>) value;
            return map;
        } else {
            throw newConfigurationException(
                processorType,
                processorTag,
                propertyName,
                "property isn't a map, but of type [" + value.getClass().getName() + "]"
            );
        }
    }

    /**
     * Returns and removes the specified property as an {@link Object} from the specified configuration map.
     */
    public static Object readObject(String processorType, String processorTag, Map<String, Object> configuration, String propertyName) {
        Object value = configuration.remove(propertyName);
        if (value == null) {
            throw newConfigurationException(processorType, processorTag, propertyName, "required property is missing");
        }
        return value;
    }

    public static OpenSearchException newConfigurationException(
        String processorType,
        String processorTag,
        String propertyName,
        String reason
    ) {
        String msg;
        if (propertyName == null) {
            msg = reason;
        } else {
            msg = "[" + propertyName + "] " + reason;
        }
        OpenSearchParseException exception = new OpenSearchParseException(msg);
        addMetadataToException(exception, processorType, processorTag, propertyName);
        return exception;
    }

    public static OpenSearchException newConfigurationException(
        String processorType,
        String processorTag,
        String propertyName,
        Exception cause
    ) {
        OpenSearchException exception = ExceptionsHelper.convertToOpenSearchException(cause);
        addMetadataToException(exception, processorType, processorTag, propertyName);
        return exception;
    }

    public static List<Processor> readProcessorConfigs(
        List<Map<String, Object>> processorConfigs,
        ScriptService scriptService,
        Map<String, Processor.Factory> processorFactories
    ) throws Exception {
        Exception exception = null;
        List<Processor> processors = new ArrayList<>();
        if (processorConfigs != null) {
            for (Map<String, Object> processorConfigWithKey : processorConfigs) {
                for (Map.Entry<String, Object> entry : processorConfigWithKey.entrySet()) {
                    try {
                        processors.add(readProcessor(processorFactories, scriptService, entry.getKey(), entry.getValue()));
                    } catch (Exception e) {
                        exception = ExceptionsHelper.useOrSuppress(exception, e);
                    }
                }
            }
        }

        if (exception != null) {
            throw exception;
        }

        return processors;
    }

    public static TemplateScript.Factory readTemplateProperty(
        String processorType,
        String processorTag,
        Map<String, Object> configuration,
        String propertyName,
        ScriptService scriptService
    ) {
        String value = readStringProperty(processorType, processorTag, configuration, propertyName, null);
        return compileTemplate(processorType, processorTag, propertyName, value, scriptService);
    }

    public static TemplateScript.Factory compileTemplate(
        String processorType,
        String processorTag,
        String propertyName,
        String propertyValue,
        ScriptService scriptService
    ) {
        try {
            // This check is here because the DEFAULT_TEMPLATE_LANG(mustache) is not
            // installed for use by REST tests. `propertyValue` will not be
            // modified if templating is not available so a script that simply returns an unmodified `propertyValue`
            // is returned.
            if (scriptService.isLangSupported(DEFAULT_TEMPLATE_LANG) && propertyValue.contains("{{")) {
                Script script = new Script(ScriptType.INLINE, DEFAULT_TEMPLATE_LANG, propertyValue, Collections.emptyMap());
                return scriptService.compile(script, TemplateScript.CONTEXT);
            } else {
                return (params) -> new TemplateScript(params) {
                    @Override
                    public String execute() {
                        return propertyValue;
                    }
                };
            }
        } catch (Exception e) {
            throw ConfigurationUtils.newConfigurationException(processorType, processorTag, propertyName, e);
        }
    }

    private static void addMetadataToException(
        OpenSearchException exception,
        String processorType,
        String processorTag,
        String propertyName
    ) {
        if (processorType != null) {
            exception.addMetadata("opensearch.processor_type", processorType);
        }
        if (processorTag != null) {
            exception.addMetadata("opensearch.processor_tag", processorTag);
        }
        if (propertyName != null) {
            exception.addMetadata("opensearch.property_name", propertyName);
        }
    }

    @SuppressWarnings("unchecked")
    public static Processor readProcessor(
        Map<String, Processor.Factory> processorFactories,
        ScriptService scriptService,
        String type,
        Object config
    ) throws Exception {
        if (config instanceof Map) {
            return readProcessor(processorFactories, scriptService, type, (Map<String, Object>) config);
        } else if (config instanceof String && "script".equals(type)) {
            Map<String, Object> normalizedScript = new HashMap<>(1);
            normalizedScript.put(ScriptType.INLINE.getParseField().getPreferredName(), config);
            return readProcessor(processorFactories, scriptService, type, normalizedScript);
        } else {
            throw newConfigurationException(type, null, null, "property isn't a map, but of type [" + config.getClass().getName() + "]");
        }
    }

    public static Processor readProcessor(
        Map<String, Processor.Factory> processorFactories,
        ScriptService scriptService,
        String type,
        Map<String, Object> config
    ) throws Exception {
        String tag = ConfigurationUtils.readOptionalStringProperty(null, null, config, TAG_KEY);
        String description = ConfigurationUtils.readOptionalStringProperty(null, tag, config, DESCRIPTION_KEY);
        boolean ignoreFailure = ConfigurationUtils.readBooleanProperty(null, null, config, IGNORE_FAILURE_KEY, false);
        Script conditionalScript = extractConditional(config);
        Processor.Factory factory = processorFactories.get(type);

        if (factory != null) {
            List<Map<String, Object>> onFailureProcessorConfigs = ConfigurationUtils.readOptionalList(
                null,
                null,
                config,
                Pipeline.ON_FAILURE_KEY
            );

            List<Processor> onFailureProcessors = readProcessorConfigs(onFailureProcessorConfigs, scriptService, processorFactories);

            if (onFailureProcessorConfigs != null && onFailureProcessors.isEmpty()) {
                throw newConfigurationException(type, tag, Pipeline.ON_FAILURE_KEY, "processors list cannot be empty");
            }

            try {
                Processor processor = factory.create(processorFactories, tag, description, config);
                if (config.isEmpty() == false) {
                    throw new OpenSearchParseException(
                        "processor [{}] doesn't support one or more provided configuration parameters {}",
                        type,
                        Arrays.toString(config.keySet().toArray())
                    );
                }
                if (onFailureProcessors.size() > 0 || ignoreFailure) {
                    processor = new CompoundProcessor(ignoreFailure, Collections.singletonList(processor), onFailureProcessors);
                }
                if (conditionalScript != null) {
                    processor = new ConditionalProcessor(tag, description, conditionalScript, scriptService, processor);
                }
                return processor;
            } catch (Exception e) {
                throw newConfigurationException(type, tag, null, e);
            }
        }
        throw newConfigurationException(type, tag, null, "No processor type exists with name [" + type + "]");
    }

    private static Script extractConditional(Map<String, Object> config) throws IOException {
        Object scriptSource = config.remove("if");
        if (scriptSource != null) {
            try (
                XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent).map(normalizeScript(scriptSource));
                InputStream stream = BytesReference.bytes(builder).streamInput();
                XContentParser parser = XContentType.JSON.xContent()
                    .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, stream)
            ) {
                return Script.parse(parser);
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> normalizeScript(Object scriptConfig) {
        if (scriptConfig instanceof Map<?, ?>) {
            return (Map<String, Object>) scriptConfig;
        } else if (scriptConfig instanceof String) {
            return Collections.singletonMap("source", scriptConfig);
        } else {
            throw newConfigurationException(
                "conditional",
                null,
                "script",
                "property isn't a map or string, but of type [" + scriptConfig.getClass().getName() + "]"
            );
        }
    }
}
