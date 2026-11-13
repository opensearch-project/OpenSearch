/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.settings;

import org.opensearch.common.Nullable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Declared constraints of a single registered setting: the value type, and the inclusive minimum and maximum when the
 * setting declares them. Exists only to marshal the settings describe responses; it is derived from, not stored on,
 * {@link Setting}. Not serializable: the responses that carry it are local-only.
 * @opensearch.internal
 */
public final class SettingValidation implements ToXContentObject {
    private final String type;
    @Nullable
    private final String min;
    @Nullable
    private final String max;

    private SettingValidation(String type, @Nullable String min, @Nullable String max) {
        this.type = Objects.requireNonNull(type, "type");
        this.min = min;
        this.max = max;
    }

    /**
     * Constraints for a setting whose value type is known.
     *
     * @param type the value type, for example {@code integer}, {@code boolean} or {@code byte_size}
     * @param min  the inclusive lower bound in its string form, or {@code null} when none is declared
     * @param max  the inclusive upper bound in its string form, or {@code null} when none is declared
     */
    public static SettingValidation of(String type, @Nullable String min, @Nullable String max) {
        return new SettingValidation(type, min, max);
    }

    /** Constraints for a setting whose value type is known but which declares no bounds. */
    public static SettingValidation ofType(String type) {
        return new SettingValidation(type, null, null);
    }

    public String type() {
        return type;
    }

    @Nullable
    public String min() {
        return min;
    }

    @Nullable
    public String max() {
        return max;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().field("type", type);
        if (min != null) {
            builder.field("min", min);
        }
        if (max != null) {
            builder.field("max", max);
        }
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SettingValidation that = (SettingValidation) o;
        return type.equals(that.type) && Objects.equals(min, that.min) && Objects.equals(max, that.max);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, min, max);
    }

    @Override
    public String toString() {
        return "SettingValidation{type=" + type + ", min=" + min + ", max=" + max + "}";
    }
}
