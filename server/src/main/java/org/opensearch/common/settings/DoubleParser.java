package org.opensearch.common.settings;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.function.Function;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Setting.Validator;

public class DoubleParser<T> implements Function<String, T>, Writeable {
    private Double defaultValue;
    private String key;
    private Double minValue;
    private Double maxValue;
    private EnumSet<Property> propSet;
    private Property[] properties;

    public DoubleParser(StreamInput in) throws IOException {
        // Read the key
        this.key = in.readString();
        // Read the default value
        this.defaultValue = in.readDouble();
        // Read the min value
        this.minValue = in.readDouble();
        // Read the max value
        this.maxValue = in.readDouble();
        // Read properties
        this.propSet = in.readEnumSet(Property.class);
        this.properties = propSet.toArray(Property[]::new);
    }

    private static boolean isFiltered(Property[] properties) {
        return properties != null && Arrays.asList(properties).contains(Property.Filtered);
    }

    private static Double perser(String s, double minValue, double maxValue, String key, boolean isFiltered) {
        double value = Double.parseDouble(s);
        if(value < minValue) {
            String err = "Failed to parse value" + (isFiltered ? "" : " [" + s + "]") + " for setting [" + key + "] must be >= " + minValue;
            throw new IllegalArgumentException(err);
        }
        if (value > maxValue) {
            String err = "Failed to parse value" + (isFiltered ? "" : " [" + s + "]") + " for setting [" + key + "] must be <= " + maxValue;
            throw new IllegalArgumentException(err);
        }
        return value;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(key);
        out.writeDouble(defaultValue);
        out.writeDouble(minValue);
        out.writeDouble(maxValue);
        out.writeEnumSet(propSet);

    }

    @Override
    public T apply(String s) {
        return parser(s, minValue, maxValue, key, isFiltered(properties));
    }

}
