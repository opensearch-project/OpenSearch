/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity.authz;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Wrapper for {@link CheckableParameter} with {@link #writeTo(StreamOutput)} implementation dependent on the parameter type.
 *
 * @opensearch.experimental
 */
public class WriteableParameter implements Writeable {

    /**
     * The Generic Types which this class can serialize.
     */
    public enum ParameterType {
        Boolean,
        Integer,
        Long,
        Float,
        Double,
        String,
        TimeValue, // long + TimeUnit
        ByteSizeValue, // long + ByteSizeUnit
    }

    private CheckableParameter<?> parameter;
    private ParameterType type;

    /**
     * Wrap a {@link CheckableParameter}. The generic type is determined from the type of the value.
     *
     * @param parameter  The parameter to wrap. The value must not be null.
     * @throws IllegalArgumentException if the parameter has a null value.
     */
    public WriteableParameter(CheckableParameter<?> parameter) {
        this(parameter, ParameterType.valueOf(parameter.getType().getSimpleName()));
    }

    /**
     * Wrap a {@link CheckableParameter} with a specified generic type.
     *
     * @param parameter  The parameter to wrap.
     * @param type  The Generic type of the parameter.
     */
    public WriteableParameter(CheckableParameter<?> parameter, ParameterType type) {
        this.parameter = parameter;
        this.type = type;
    }

    /**
     * Wrap a {@link CheckableParameter} read from a stream.
     *
     * @param in Input to read the value from.
     * @throws IOException if there is an error reading the values
     */
    public WriteableParameter(StreamInput in) throws IOException {
        // Read the type
        this.type = in.readEnum(ParameterType.class);
        // Read the key
        String key = in.readString();
        // Read the value
        Object value = readValue(in);
        this.parameter = createParameter(type, key, value);
    }

    /**
     * Gets the wrapped setting. Use {@link #getType()} to determine its generic type.
     *
     * @return The wrapped setting.
     */
    public CheckableParameter<?> getParameter() {
        return this.parameter;
    }

    /**
     * Gets the generic type of the wrapped setting.
     *
     * @return The wrapped setting's generic type.
     */
    public ParameterType getType() {
        return this.type;
    }

    @SuppressWarnings("unchecked")
    private CheckableParameter<?> createParameter(ParameterType type, String key, Object value) {
        switch (type) {
            case Boolean:
                return new CheckableParameter<>(key, (Boolean) value, Boolean.class);
            case Integer:
                return new CheckableParameter<>(key, (Integer) value, Integer.class);
            case Long:
                return new CheckableParameter<>(key, (Long) value, Long.class);
            case Float:
                return new CheckableParameter<>(key, (Float) value, Float.class);
            case Double:
                return new CheckableParameter<>(key, (Double) value, Double.class);
            case String:
                return new CheckableParameter<>(key, (String) value, String.class);
            case TimeValue:
                return new CheckableParameter<>(key, (TimeValue) value, TimeValue.class);
            case ByteSizeValue:
                return new CheckableParameter<>(key, (ByteSizeValue) value, ByteSizeValue.class);
            default:
                // This Should Never Happen (TM)
                throw new UnsupportedOperationException("A ParameterType has been added to the enum and not handled here.");
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // Write the type
        out.writeEnum(type);
        // Write the key
        out.writeString(parameter.getKey());
        // Write the value
        writeValue(out, parameter.getValue());
    }

    private void writeValue(StreamOutput out, Object value) throws IOException {
        switch (type) {
            case Boolean:
                out.writeBoolean((boolean) value);
                break;
            case Integer:
                out.writeInt((int) value);
                break;
            case Long:
                out.writeLong((long) value);
                break;
            case Float:
                out.writeFloat((float) value);
                break;
            case Double:
                out.writeDouble((double) value);
                break;
            case String:
                out.writeString((String) value);
                break;
            case TimeValue:
                TimeValue tv = (TimeValue) value;
                out.writeLong(tv.duration());
                out.writeString(tv.timeUnit().name());
                break;
            case ByteSizeValue:
                ((ByteSizeValue) value).writeTo(out);
                break;
            default:
                // This Should Never Happen (TM)
                throw new UnsupportedOperationException("A ParameterType has been added to the enum and not handled here.");
        }
    }

    private Object readValue(StreamInput in) throws IOException {
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
                throw new UnsupportedOperationException("A ParameterType has been added to the enum and not handled here.");
        }
    }

    @Override
    public String toString() {
        return "WriteableParameter{type=Parameter<" + type + ">, parameter=" + parameter + "}";
    }
}
