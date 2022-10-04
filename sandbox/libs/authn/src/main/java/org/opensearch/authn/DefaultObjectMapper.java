/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.authn;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;

/**
 * @opensearch.experimental
 */
public class DefaultObjectMapper {
    public static final ObjectMapper objectMapper = new ObjectMapper();
    public final static ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());
    private static final ObjectMapper defaulOmittingObjectMapper = new ObjectMapper();

    static {
        objectMapper.setSerializationInclusion(Include.NON_NULL);
        // objectMapper.enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
        objectMapper.enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
        defaulOmittingObjectMapper.setSerializationInclusion(Include.NON_DEFAULT);
        defaulOmittingObjectMapper.enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
        YAML_MAPPER.enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
    }

    public static void inject(final InjectableValues.Std injectableValues) {
        objectMapper.setInjectableValues(injectableValues);
        YAML_MAPPER.setInjectableValues(injectableValues);
        defaulOmittingObjectMapper.setInjectableValues(injectableValues);
    }

    public static <T> T readValue(String string, Class<T> clazz) throws IOException {
        return objectMapper.readValue(string, clazz);
    }

    public static String writeValueAsString(Object value, boolean omitDefaults) throws JsonProcessingException {
        return (omitDefaults ? defaulOmittingObjectMapper : objectMapper).writeValueAsString(value);
    }
}
