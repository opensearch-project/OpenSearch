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
    private Setting<Double> setting;
    private Double minValue;
    private Double maxValue;
    private Double fallback;

    public DoubleParser(Setting<Double> setting) {
        this.setting = setting;
    }

    public DoubleParser(StreamInput in) throws IOException {
        super();
        // Read the key
        String key = in.readString();
        // Read the default value
        Double defaultValue = in.readDouble();
        // Read the min value
        this.minValue = in.readDouble();
        // Read the max value
        this.maxValue = in.readDouble();
        // Read the fallback
        boolean hasFallback = in.readBoolean();
        if (hasFallback) {
            this.fallback = in.readDouble();
        }
        // Read properties
        EnumSet<Property> propSet = in.readEnumSet(Property.class);
        this.setting = doubleSetting(key, defaultValue, minValue, maxValue, propSet.toArray(Property[]::new));

    }

    public static Setting<Double> doubleSetting(
        String key,
        double defaultValue,
        double minValue,
        double maxValue,
        Property[] properties
    ) {
        return new Setting<Double>(
            key,
            Double.toString(defaultValue),
            (s) -> doubleParser(s, minValue, maxValue, key, isFiltered(properties)),
            v -> {},
            properties
        );
    }

    private static boolean isFiltered(Property[] properties) {
        return properties != null && Arrays.asList(properties).contains(Property.Filtered);
    }

    private static double doubleParser(String s, double minValue, double maxValue, String key, boolean isFiltered) {
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
        out.writeString(setting.getKey());
        out.writeDouble(setting.getDefault(Settings.EMPTY));
        out.writeDouble(minValue);
        out.writeDouble(maxValue);
        boolean hasFallback = setting.fallbackSetting != null;
        out.writeBoolean(hasFallback);
        if (hasFallback) {
            out.writeDouble(fallback);
        }
        out.writeEnumSet(setting.getProperties());

    }

    @Override
    public T apply(String s) {
        // TODO Auto-generated method stub
        return new doubleParser(key, defaultValue, minValue, maxValue, properties);
    }

}
