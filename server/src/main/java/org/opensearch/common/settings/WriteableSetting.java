/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.settings;

import org.opensearch.Version;
import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.common.settings.Setting.ByteSizeValueParser;
import org.opensearch.common.settings.Setting.DoubleParser;
import org.opensearch.common.settings.Setting.FloatParser;
import org.opensearch.common.settings.Setting.IntegerParser;
import org.opensearch.common.settings.Setting.LongParser;
import org.opensearch.common.settings.Setting.MemorySizeValueParser;
import org.opensearch.common.settings.Setting.MinMaxTimeValueParser;
import org.opensearch.common.settings.Setting.MinTimeValueParser;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Setting.RegexValidator;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
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
    public enum SettingType {
        Boolean,
        Integer,
        Long,
        Float,
        Double,
        String,
        TimeValue, // long + TimeUnit
        ByteSizeValue, // long + ByteSizeUnit
        Version
    }

    private Setting<?> setting;
    private SettingType type;

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
    public WriteableSetting(Setting<?> setting, SettingType type) {
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
        this.type = in.readEnum(SettingType.class);
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
        // Read the parser
        Object parser = null;
        boolean isParserWriteable = in.readBoolean();
        if (isParserWriteable) {
            parser = readParser(in, parser);
        }
        // Read the Validator
        Object validator = new Object();
        boolean isValidatorWriteable = in.readBoolean();
        if (isValidatorWriteable) {
            validator = readValidator(in);
        }
        // Read properties
        EnumSet<Property> propSet = in.readEnumSet(Property.class);
        // Put it all in a setting object
        this.setting = createSetting(type, key, defaultValue, parser, validator, fallback, propSet.toArray(Property[]::new));
    }

    /**
     * Due to type erasure, it is impossible to determine the generic type of a Setting at runtime.
     * All settings have a non-null default, however, so the type of the default can be used to determine the setting type.
     *
     * @param setting The setting with a generic type.
     * @return The corresponding {@link SettingType} for the default value.
     */
    private static SettingType getGenericTypeFromDefault(Setting<?> setting) {
        String typeStr = setting.getDefault(Settings.EMPTY).getClass().getSimpleName();
        try {
            // This throws IAE if not in enum
            return SettingType.valueOf(typeStr);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "This class is not yet set up to handle the generic type: "
                    + typeStr
                    + ". Supported types are "
                    + Arrays.toString(SettingType.values())
            );
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
    public SettingType getType() {
        return this.type;
    }

    @SuppressWarnings("unchecked")
    private Setting<?> createSetting(
        SettingType type,
        String key,
        Object defaultValue,
        @Nullable Object parser,
        Object validator,
        WriteableSetting fallback,
        Property[] propertyArray
    ) {
        switch (type) {
            case Boolean:
                return fallback == null
                    ? Setting.boolSetting(key, (boolean) defaultValue, propertyArray)
                    : Setting.boolSetting(key, (Setting<Boolean>) fallback.getSetting(), propertyArray);
            case Integer:
                if (fallback == null && parser instanceof Writeable) {
                    return Setting.intSetting(
                        key,
                        (int) defaultValue,
                        ((IntegerParser) parser).getMin(),
                        ((IntegerParser) parser).getMax(),
                        propertyArray
                    );
                } else if (fallback == null) {
                    return Setting.intSetting(key, (int) defaultValue, propertyArray);
                }
                return Setting.intSetting(key, (Setting<Integer>) fallback.getSetting(), propertyArray);
            case Long:
                if (fallback == null && parser instanceof Writeable) {
                    return Setting.longSetting(
                        key,
                        (long) defaultValue,
                        ((LongParser) parser).getMin(),
                        ((LongParser) parser).getMax(),
                        propertyArray
                    );
                } else if (fallback == null) {
                    return Setting.longSetting(key, (long) defaultValue, propertyArray);
                }
                return Setting.longSetting(key, (Setting<Long>) fallback.getSetting(), propertyArray);
            case Float:
                if (fallback == null && parser instanceof Writeable) {
                    return Setting.floatSetting(
                        key,
                        (float) defaultValue,
                        ((FloatParser) parser).getMin(),
                        ((FloatParser) parser).getMax(),
                        propertyArray
                    );
                } else if (fallback == null) {
                    return Setting.floatSetting(key, (float) defaultValue, propertyArray);
                }
                return Setting.floatSetting(key, (Setting<Float>) fallback.getSetting(), propertyArray);
            case Double:
                if (fallback == null && parser instanceof Writeable) {
                    return Setting.doubleSetting(
                        key,
                        (double) defaultValue,
                        ((DoubleParser) parser).getMin(),
                        ((DoubleParser) parser).getMax(),
                        propertyArray
                    );
                } else if (fallback == null) {
                    return Setting.doubleSetting(key, (double) defaultValue, propertyArray);
                }
                return Setting.doubleSetting(key, (Setting<Double>) fallback.getSetting(), propertyArray);
            case String:
                return fallback == null
                    ? Setting.simpleString(key, (String) defaultValue, propertyArray)
                    : Setting.simpleString(key, (Setting<String>) fallback.getSetting(), propertyArray);
            case TimeValue:
                if (fallback == null && parser instanceof Writeable) {
                    if (parser instanceof MinMaxTimeValueParser) {
                        return Setting.timeSetting(
                            key,
                            (TimeValue) defaultValue,
                            ((MinMaxTimeValueParser) parser).getMin(),
                            ((MinMaxTimeValueParser) parser).getMax(),
                            propertyArray
                        );
                    } else {
                        return Setting.timeSetting(key, (TimeValue) defaultValue, ((MinTimeValueParser) parser).getMin(), propertyArray);
                    }
                } else if (fallback == null) {
                    return Setting.timeSetting(key, (TimeValue) defaultValue, propertyArray);
                }
                return Setting.timeSetting(key, (Setting<TimeValue>) fallback.getSetting(), propertyArray);
            case ByteSizeValue:
                if (fallback == null && parser instanceof Writeable) {
                    if (parser instanceof MemorySizeValueParser) {
                        return Setting.memorySizeSetting(key, (ByteSizeValue) defaultValue, propertyArray);
                    } else {
                        ByteSizeValueParser byteSizeValueParser = (ByteSizeValueParser) parser;
                        return Setting.byteSizeSetting(
                            key,
                            (ByteSizeValue) defaultValue,
                            byteSizeValueParser.getMin(),
                            byteSizeValueParser.getMax(),
                            propertyArray
                        );
                    }
                } else if (fallback == null) {
                    return Setting.byteSizeSetting(key, (ByteSizeValue) defaultValue, propertyArray);
                }
                return Setting.byteSizeSetting(key, (Setting<ByteSizeValue>) fallback.getSetting(), propertyArray);
            case Version:
                // No fallback option on this method
                return Setting.versionSetting(key, (Version) defaultValue, propertyArray);
            default:
                // This Should Never Happen (TM)
                throw new IllegalArgumentException("A SettingType has been added to the enum and not handled here.");
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // Write the type
        out.writeEnum(type);
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
        // Write a boolean specifying whether the parser is an instanceof writeable
        boolean isParserWriteable = setting.parser instanceof Writeable;
        out.writeBoolean(isParserWriteable);
        if (isParserWriteable) {
            writeParser(out, setting.parser);
        }
        // Write the validator
        boolean isValidatorWriteable = setting.validator instanceof Writeable;
        out.writeBoolean(isValidatorWriteable);
        if (isValidatorWriteable) {
            writeValidator(out, setting.validator);
        }
        // Write properties
        out.writeEnumSet(setting.getProperties());
    }

    private void writeParser(StreamOutput out, Object parser) throws IOException {
        switch (type) {
            case Integer:
                ((IntegerParser) parser).writeTo(out);
                break;
            case Long:
                ((LongParser) parser).writeTo(out);
                break;
            case Float:
                ((FloatParser) parser).writeTo(out);
                break;
            case Double:
                ((DoubleParser) parser).writeTo(out);
                break;
            case TimeValue:
                if (parser instanceof MinMaxTimeValueParser) {
                    out.writeBoolean(true);
                    ((MinMaxTimeValueParser) parser).writeTo(out);
                } else if (parser instanceof MinTimeValueParser) {
                    out.writeBoolean(false);
                    ((MinTimeValueParser) parser).writeTo(out);
                }
                break;
            case ByteSizeValue:
                if (parser instanceof ByteSizeValueParser) {
                    out.writeBoolean(true);
                    ((ByteSizeValueParser) parser).writeTo(out);
                } else if (parser instanceof MemorySizeValueParser) {
                    out.writeBoolean(false);
                    ((MemorySizeValueParser) parser).writeTo(out);
                }
                break;
            default:
                throw new IllegalArgumentException("A SettingType has been added to the enum and not handled here.");
        }
    }

    private void writeValidator(StreamOutput out, Object validator) throws IOException {
        ((Writeable) validator).writeTo(out);
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
            case Version:
                out.writeVersion((Version) defaultValue);
                break;
            default:
                // This Should Never Happen (TM)
                throw new IllegalArgumentException("A SettingType has been added to the enum and not handled here.");
        }
    }

    private Object readParser(StreamInput in, Object parser) throws IOException {
        switch (type) {
            case Integer:
                return new IntegerParser(in);
            case Long:
                return new LongParser(in);
            case Float:
                return new FloatParser(in);
            case Double:
                return new DoubleParser(in);
            case TimeValue:
                if (in.readBoolean()) {
                    return new MinMaxTimeValueParser(in);
                } else {
                    return new MinTimeValueParser(in);
                }
            case ByteSizeValue:
                if (in.readBoolean()) {
                    return new ByteSizeValueParser(in);
                } else {
                    return new MemorySizeValueParser(in);
                }
            default:
                throw new IllegalArgumentException("A SettingType has been added to the enum and not handled here.");
        }
    }

    private Object readValidator(StreamInput in) throws IOException {
        return new RegexValidator(in);
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
            case Version:
                return in.readVersion();
            default:
                // This Should Never Happen (TM)
                throw new IllegalArgumentException("A SettingType has been added to the enum and not handled here.");
        }
    }

    @Override
    public String toString() {
        return "WriteableSettings{type=Setting<" + type + ">, setting=" + setting + "}";
    }
}
