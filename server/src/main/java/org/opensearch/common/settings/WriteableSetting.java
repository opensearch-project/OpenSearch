/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.settings;

import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamInput;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Wrapper for {@link Setting} with {@link #writeTo(StreamOutput)} implementation dependent on the setting type.
 *
 * @opensearch.internal
 */
public class WriteableSetting implements Writeable {

    /**
     * The Generic Types which this class can serialize.
     */
    public enum WriteableSettingGenericType {
        Boolean,
        Integer,
        Long,
        Float,
        Double,
        String,
        TimeValue, // long + TimeUnit
        ByteSizeValue // long + ByteSizeUnit
    }

    private Setting<?> setting;
    private WriteableSettingGenericType type;

    /**
     * Wrap a {@link Setting}. The generic type is determined from the type of the default value.
     *
     * @param setting  The setting to wrap. The default value must not be null.
     * @throws IllegalArgumentException if the setting has a null default value.
     */
    public WriteableSetting(Setting<?> setting) {
        this(setting, getGenericTypeFromDefault(setting));
    }

    /**
     * Wrap a {@link Setting} with a specified generic type.
     *
     * @param setting  The setting to wrap.
     * @param type  The Generic type of the setting.
     */
    public WriteableSetting(Setting<?> setting, WriteableSettingGenericType type) {
        this.setting = setting;
        this.type = type;
    }

    /**
     * Wrap a {@link Setting} read from a stream.
     *
     * @param in Input to read the value from.
     * @throws IOException if there is an error reading the values
     */
    public WriteableSetting(StreamInput in) throws IOException {
        // Read the type
        this.type = WriteableSettingGenericType.valueOf(in.readString());
        // Read the key
        String key = in.readString();
        // Read the default value
        Object defaultValue = readDefaultValue(in);
        // Read a boolean specifying whether the fallback settings is null
        WriteableSetting fallback = null;
        boolean hasFallback = in.readBoolean();
        if (hasFallback) {
            fallback = new WriteableSetting(in);
        }
        // We are using known types so don't need the parser
        // We are not using validator
        // Read properties
        int size = in.readVInt();
        Property[] propArray = new Property[size];
        for (int i = 0; i < size; i++) {
            propArray[i] = Property.valueOf(in.readString());
        }
        // Put it all in a setting object
        this.setting = createSetting(type, key, defaultValue, fallback, propArray);
    }

    private static WriteableSettingGenericType getGenericTypeFromDefault(Setting<?> setting) {
        Object defaultValue = setting.getDefault(Settings.EMPTY);
        if (defaultValue == null) {
            throw new IllegalArgumentException("Unable to determine the generic type of this setting with a null default value.");
        }
        String typeStr = defaultValue.getClass().getSimpleName();
        try {
            return WriteableSettingGenericType.valueOf(typeStr);
        } catch (IllegalArgumentException e) {
            throw new UnsupportedOperationException("This class is not yet set up to handle the Generic Type: " + typeStr);
        }
    }

    /**
     * Gets the wrapped setting. Use {@link #getType()} to determine its generic type.
     *
     * @return The wrapped setting.
     */
    public Setting<?> getSetting() {
        return this.setting;
    }

    /**
     * Gets the generic type of the wrapped setting.
     *
     * @return The wrapped setting's generic type.
     */
    public WriteableSettingGenericType getType() {
        return this.type;
    }

    @SuppressWarnings("unchecked")
    private Setting<?> createSetting(
        WriteableSettingGenericType genericType,
        String key,
        Object defaultValue,
        WriteableSetting fallback,
        Property[] propArray
    ) {
        switch (genericType) {
            case Boolean:
                return fallback == null
                    ? Setting.boolSetting(key, (boolean) defaultValue, propArray)
                    : Setting.boolSetting(key, (Setting<Boolean>) fallback.getSetting(), propArray);
            case Integer:
                return fallback == null
                    ? Setting.intSetting(key, (int) defaultValue, propArray)
                    : Setting.intSetting(key, (Setting<Integer>) fallback.getSetting(), propArray);
            case Long:
                return fallback == null
                    ? Setting.longSetting(key, (long) defaultValue, propArray)
                    : Setting.longSetting(key, (Setting<Long>) fallback.getSetting(), propArray);
            case Float:
                return fallback == null
                    ? Setting.floatSetting(key, (float) defaultValue, propArray)
                    : Setting.floatSetting(key, (Setting<Float>) fallback.getSetting(), propArray);
            case Double:
                return fallback == null
                    ? Setting.doubleSetting(key, (double) defaultValue, propArray)
                    : Setting.doubleSetting(key, (Setting<Double>) fallback.getSetting(), propArray);
            case String:
                return fallback == null
                    ? Setting.simpleString(key, (String) defaultValue, propArray)
                    : Setting.simpleString(key, (Setting<String>) fallback.getSetting(), propArray);
            case TimeValue:
                return fallback == null
                    ? Setting.timeSetting(key, (TimeValue) defaultValue, propArray)
                    : Setting.timeSetting(key, (Setting<TimeValue>) fallback.getSetting(), propArray);
            case ByteSizeValue:
                return fallback == null
                    ? Setting.byteSizeSetting(key, (ByteSizeValue) defaultValue, propArray)
                    : Setting.byteSizeSetting(key, (Setting<ByteSizeValue>) fallback.getSetting(), propArray);
            default:
                // This Should Never Happen (TM)
                throw new UnsupportedOperationException("A WriteableSettingGenericType has been added to the enum and not handled here.");
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // Write the type
        out.writeString(type.name());
        // Write the key
        out.writeString(setting.getKey());
        // Write the default value
        writeDefaultValue(out, setting.getDefault(Settings.EMPTY));
        // Write a boolean specifying whether the fallback settings is null
        boolean hasFallback = setting.fallbackSetting != null;
        out.writeBoolean(hasFallback);
        if (hasFallback) {
            new WriteableSetting(setting.fallbackSetting, type).writeTo(out);
        }
        // We are using known types so don't need the parser
        // We are not using validator
        // Write properties
        out.writeVInt(setting.getProperties().size());
        for (Property prop : setting.getProperties()) {
            out.writeString(prop.name());
        }
    }

    private void writeDefaultValue(StreamOutput out, Object defaultValue) throws IOException {
        switch (type) {
            case Boolean:
                out.writeBoolean((boolean) defaultValue);
                break;
            case Integer:
                out.writeInt((int) defaultValue);
                break;
            case Long:
                out.writeLong((long) defaultValue);
                break;
            case Float:
                out.writeFloat((float) defaultValue);
                break;
            case Double:
                out.writeDouble((double) defaultValue);
                break;
            case String:
                out.writeString((String) defaultValue);
                break;
            case TimeValue:
                TimeValue tv = (TimeValue) defaultValue;
                out.writeLong(tv.duration());
                out.writeString(tv.timeUnit().name());
                break;
            case ByteSizeValue:
                ((ByteSizeValue) defaultValue).writeTo(out);
                break;
            default:
                // This Should Never Happen (TM)
                throw new UnsupportedOperationException("A WriteableSettingGenericType has been added to the enum and not handled here.");
        }
    }

    private Object readDefaultValue(StreamInput in) throws IOException {
        switch (type) {
            case Boolean:
                return in.readBoolean();
            case Integer:
                return in.readInt();
            case Long:
                return in.readLong();
            case Float:
                return in.readFloat();
            case Double:
                return in.readDouble();
            case String:
                return in.readString();
            case TimeValue:
                long duration = in.readLong();
                TimeUnit unit = TimeUnit.valueOf(in.readString());
                return new TimeValue(duration, unit);
            case ByteSizeValue:
                return new ByteSizeValue(in);
            default:
                // This Should Never Happen (TM)
                throw new UnsupportedOperationException("A WriteableSettingGenericType has been added to the enum and not handled here.");
        }
    }

    @Override
    public String toString() {
        return "WriteableSettings{type=Setting<" + type + ">, setting=" + setting + "}";
    }

    public static void main(String... args) throws IOException {
        // TEMPORARY, this will be moved to a test class

        // LegacyOpenDistroAnomalyDetectorSettings.MAX_RETRY_FOR_END_RUN_EXCEPTION
        // Note this provides default value of 6
        Setting<Integer> fallbackSetting = Setting.intSetting(
            "opendistro.anomaly_detection.max_retry_for_end_run_exception",
            6,
            0,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic // ,
            // Fails if we try to use it
            // Setting.Property.Deprecated
        );

        System.out.println("Original with fallback");
        // AnomalyDetectorSetting.MAX_RETRY_FOR_END_RUN_EXCEPTION
        // Note this falls back to the other setting
        // This populates the default value since we have no settings, although it won't be used
        Setting<Integer> intSetting = Setting.intSetting(
            "plugins.anomaly_detection.max_retry_for_end_run_exception",
            fallbackSetting,
            0,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );
        System.out.println("Original");
        System.out.println(intSetting.toString());

        WriteableSetting wsOut = new WriteableSetting(intSetting);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            wsOut.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                WriteableSetting wsIn = new WriteableSetting(in);
                System.out.println("After transport across a (byte)stream");
                System.out.println(wsIn.toString());
            }
        }
    }
}
