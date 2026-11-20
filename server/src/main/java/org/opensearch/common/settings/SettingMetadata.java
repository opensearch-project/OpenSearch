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
 * Response-only projection of a single registered setting as reported by the cluster and index settings describe APIs.
 * Deliberately separate from {@link Setting}, which holds parser and validator functions. Not serializable: the responses
 * that carry it are local-only.
 * @opensearch.internal
 */
public final class SettingMetadata implements ToXContentObject {
    private final boolean dynamic;
    @Nullable
    private final String defaultValue;
    @Nullable
    private final SettingValidation validation;
    @Nullable
    private final String description;

    /**
     * @param dynamic      whether the setting can be updated on a running cluster
     * @param defaultValue the declared default in its string form (see {@link SettingsDescriber} for what "declared" covers), or
     *                     {@code null} when none should be reported
     * @param validation   the declared value type and bounds, or {@code null} when neither is known
     * @param description  the declared description, or {@code null} when none has been written
     */
    private SettingMetadata(
        boolean dynamic,
        @Nullable String defaultValue,
        @Nullable SettingValidation validation,
        @Nullable String description
    ) {
        this.dynamic = dynamic;
        this.defaultValue = defaultValue;
        this.validation = validation;
        this.description = description;
    }

    /** Metadata for a setting whose default is not reported. */
    public static SettingMetadata withoutDefault(boolean dynamic) {
        return new SettingMetadata(dynamic, null, null, null);
    }

    /** Metadata for a setting with a declared default. */
    public static SettingMetadata withDefault(boolean dynamic, String defaultValue) {
        return new SettingMetadata(dynamic, Objects.requireNonNull(defaultValue, "defaultValue"), null, null);
    }

    /** Returns a copy of this metadata carrying the given validation metadata; {@code null} leaves {@code validation} unset. */
    public SettingMetadata withValidation(@Nullable SettingValidation validation) {
        return new SettingMetadata(dynamic, defaultValue, validation, description);
    }

    /** Returns a copy of this metadata carrying the given description; {@code null} leaves it unset. */
    public SettingMetadata withDescription(@Nullable String description) {
        return new SettingMetadata(dynamic, defaultValue, validation, description);
    }

    public boolean dynamic() {
        return dynamic;
    }

    @Nullable
    public String defaultValue() {
        return defaultValue;
    }

    @Nullable
    public SettingValidation validation() {
        return validation;
    }

    @Nullable
    public String description() {
        return description;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().field("dynamic", dynamic);
        if (defaultValue != null) {
            builder.field("default", defaultValue);
        }
        if (validation != null) {
            builder.field("validation");
            validation.toXContent(builder, params);
        }
        if (description != null) {
            builder.field("description", description);
        }
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SettingMetadata that = (SettingMetadata) o;
        return dynamic == that.dynamic
            && Objects.equals(defaultValue, that.defaultValue)
            && Objects.equals(validation, that.validation)
            && Objects.equals(description, that.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dynamic, defaultValue, validation, description);
    }

    @Override
    public String toString() {
        return "SettingMetadata{dynamic="
            + dynamic
            + ", default="
            + defaultValue
            + ", validation="
            + validation
            + ", description="
            + description
            + "}";
    }
}
