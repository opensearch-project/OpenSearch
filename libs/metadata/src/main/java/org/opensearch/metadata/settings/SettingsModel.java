/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.metadata.settings;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * A model class for Settings that can be used in metadata lib without
 * depending on the full Settings class from server module.
 */
@ExperimentalApi
public final class SettingsModel implements Writeable, ToXContentFragment {

    /**
     * An empty SettingsModel instance.
     */
    public static final SettingsModel EMPTY = new SettingsModel(Collections.emptyMap());

    private final Map<String, Object> settings;

    /**
     * Creates a SettingsModel from a map.
     * Values can be String, List&lt;String&gt;, or null.
     * Non-null, non-list values are converted to String for consistency.
     *
     * @param settings the settings map
     */
    public SettingsModel(Map<String, Object> settings) {
        this.settings = normalizeSettings(settings);
    }

    /**
     * Creates a SettingsModel by reading from StreamInput.
     * Wire format is compatible with Settings.readSettingsFromStream.
     * Non-null, non-list values are converted to String for consistency with Settings.
     *
     * @param in the stream to read from
     * @throws IOException if an I/O error occurs
     */
    public SettingsModel(StreamInput in) throws IOException {
        int numberOfSettings = in.readVInt();
        Map<String, Object> map = new HashMap<>(numberOfSettings);
        for (int i = 0; i < numberOfSettings; i++) {
            map.put(in.readString(), in.readGenericValue());
        }
        this.settings = normalizeSettings(map);
    }

    /**
     * Normalizes settings map by converting non-null, non-list values to String.
     * Returns an unmodifiable sorted map.
     *
     * @param settings the settings map to normalize
     * @return normalized unmodifiable sorted map
     */
    private static Map<String, Object> normalizeSettings(Map<String, Object> settings) {
        if (settings == null || settings.isEmpty()) {
            return Collections.emptySortedMap();
        }

        TreeMap<String, Object> normalized = new TreeMap<>();
        for (Map.Entry<String, Object> entry : settings.entrySet()) {
            Object value = entry.getValue();
            // Convert non-null, non-list values to String for consistency
            if (value != null && !(value instanceof List)) {
                value = value.toString();
            }
            normalized.put(entry.getKey(), value);
        }
        return Collections.unmodifiableSortedMap(normalized);
    }

    /**
     * Writes to StreamOutput.
     * Wire format is compatible with Settings.writeSettingsToStream.
     *
     * @param out the stream to write to
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(settings.size());
        for (Map.Entry<String, Object> entry : settings.entrySet()) {
            out.writeString(entry.getKey());
            out.writeGenericValue(entry.getValue());
        }
    }

    /**
     * Returns the raw settings map.
     *
     * @return an unmodifiable map of settings
     */
    public Map<String, Object> getSettings() {
        return settings;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SettingsModel that = (SettingsModel) o;
        return Objects.equals(settings, that.settings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(settings);
    }

    /**
     * Parses a SettingsModel from XContent.
     * Expects the parser to be positioned at the start of a settings object.
     * Handles both flat key-value pairs and nested objects.
     *
     * @param parser the XContent parser
     * @return the parsed SettingsModel
     * @throws IOException if parsing fails
     */
    public static SettingsModel fromXContent(XContentParser parser) throws IOException {
        Map<String, Object> settings = new HashMap<>();
        parseSettingsObject(parser, settings, "");
        return new SettingsModel(settings);
    }

    /**
     * Recursively parses settings from XContent, flattening nested objects into dot-separated keys.
     */
    private static void parseSettingsObject(XContentParser parser, Map<String, Object> settings, String prefix) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                // Nested object - flatten with dot notation
                String nestedPrefix = prefix.isEmpty() ? currentFieldName : prefix + "." + currentFieldName;
                parseSettingsObject(parser, settings, nestedPrefix);
            } else if (token == XContentParser.Token.START_ARRAY) {
                // Array value - parse as list
                String key = prefix.isEmpty() ? currentFieldName : prefix + "." + currentFieldName;
                List<String> list = new ArrayList<>();
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    if (token.isValue()) {
                        list.add(parser.text());
                    }
                }
                settings.put(key, list);
            } else if (token == XContentParser.Token.VALUE_NULL) {
                // Null value
                String key = prefix.isEmpty() ? currentFieldName : prefix + "." + currentFieldName;
                settings.put(key, null);
            } else if (token.isValue()) {
                // Simple value
                String key = prefix.isEmpty() ? currentFieldName : prefix + "." + currentFieldName;
                settings.put(key, parser.text());
            }
        }
    }

    /**
     * Writes this SettingsModel to XContent.
     * Outputs settings as flat key-value pairs within the current object context.
     *
     * @param builder the XContent builder
     * @param params the ToXContent params
     * @return the XContent builder
     * @throws IOException if writing fails
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (!params.paramAsBoolean("flat_settings", false)) {
            for (Map.Entry<String, Object> entry : getAsStructuredMap().entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
        } else {
            for (Map.Entry<String, Object> entry : settings.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
        }
        return builder;
    }

    /**
     * Converts flat dot-separated settings into a nested map structure, then converts
     * numeric-keyed maps to arrays where appropriate.
     */
    private Map<String, Object> getAsStructuredMap() {
        Map<String, Object> map = new HashMap<>(2);
        for (Map.Entry<String, Object> entry : settings.entrySet()) {
            processSetting(map, "", entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getValue() instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> valMap = (Map<String, Object>) entry.getValue();
                entry.setValue(convertMapsToArrays(valMap));
            }
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    private static void processSetting(Map<String, Object> map, String prefix, String setting, Object value) {
        int prefixLength = setting.indexOf('.');
        if (prefixLength == -1) {
            Map<String, Object> innerMap = (Map<String, Object>) map.get(prefix + setting);
            if (innerMap != null) {
                for (Map.Entry<String, Object> entry : innerMap.entrySet()) {
                    map.put(prefix + setting + "." + entry.getKey(), entry.getValue());
                }
            }
            map.put(prefix + setting, value);
        } else {
            String key = setting.substring(0, prefixLength);
            String rest = setting.substring(prefixLength + 1);
            Object existingValue = map.get(prefix + key);
            if (existingValue == null) {
                Map<String, Object> newMap = new HashMap<>(2);
                processSetting(newMap, "", rest, value);
                map.put(prefix + key, newMap);
            } else if (existingValue instanceof Map) {
                Map<String, Object> innerMap = (Map<String, Object>) existingValue;
                processSetting(innerMap, "", rest, value);
                map.put(prefix + key, innerMap);
            } else {
                processSetting(map, prefix + key + ".", rest, value);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static Object convertMapsToArrays(Map<String, Object> map) {
        if (map.isEmpty()) {
            return map;
        }
        boolean isArray = true;
        int maxIndex = -1;
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (isArray) {
                try {
                    int index = Integer.parseInt(entry.getKey());
                    if (index >= 0) {
                        maxIndex = Math.max(maxIndex, index);
                    } else {
                        isArray = false;
                    }
                } catch (NumberFormatException ex) {
                    isArray = false;
                }
            }
            if (entry.getValue() instanceof Map) {
                Map<String, Object> valMap = (Map<String, Object>) entry.getValue();
                entry.setValue(convertMapsToArrays(valMap));
            }
        }
        if (isArray && (maxIndex + 1) == map.size()) {
            ArrayList<Object> newValue = new ArrayList<>(maxIndex + 1);
            for (int i = 0; i <= maxIndex; i++) {
                Object obj = map.get(Integer.toString(i));
                if (obj == null) {
                    return map;
                }
                newValue.add(obj);
            }
            return newValue;
        }
        return map;
    }
}
