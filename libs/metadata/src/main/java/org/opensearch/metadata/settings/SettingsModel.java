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

import java.io.IOException;
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
public final class SettingsModel implements Writeable {

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
}
