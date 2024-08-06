/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.ingest.common;

import org.opensearch.common.network.InetAddresses;
import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.ingest.ConfigurationUtils.newConfigurationException;

/**
 * Processor that converts fields content to a different type. Supported types are: integer, float, boolean and string.
 * Throws exception if the field is not there or the conversion fails.
 */
public final class ConvertProcessor extends AbstractProcessor {

    enum Type {
        INTEGER {
            @Override
            public Object convert(Object value) {
                try {
                    String strValue = value.toString();
                    if (strValue.startsWith("0x") || strValue.startsWith("-0x")) {
                        return Integer.decode(strValue);
                    }
                    return Integer.parseInt(strValue);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("unable to convert [" + value + "] to integer", e);
                }

            }
        },
        LONG {
            @Override
            public Object convert(Object value) {
                try {
                    String strValue = value.toString();
                    if (strValue.startsWith("0x") || strValue.startsWith("-0x")) {
                        return Long.decode(strValue);
                    }
                    return Long.parseLong(strValue);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("unable to convert [" + value + "] to long", e);
                }
            }
        },
        DOUBLE {
            @Override
            public Object convert(Object value) {
                try {
                    return Double.parseDouble(value.toString());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("unable to convert [" + value + "] to double", e);
                }
            }
        },
        FLOAT {
            @Override
            public Object convert(Object value) {
                try {
                    return Float.parseFloat(value.toString());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("unable to convert [" + value + "] to float", e);
                }
            }
        },
        BOOLEAN {
            @Override
            public Object convert(Object value) {
                if (value.toString().equalsIgnoreCase("true")) {
                    return true;
                } else if (value.toString().equalsIgnoreCase("false")) {
                    return false;
                } else {
                    throw new IllegalArgumentException("[" + value + "] is not a boolean value, cannot convert to boolean");
                }
            }
        },
        STRING {
            @Override
            public Object convert(Object value) {
                return value.toString();
            }
        },
        IP {
            @Override
            public Object convert(Object value) {
                // If the value is a valid ipv4/ipv6 address, we return the original value directly because IpFieldType
                // can accept string value, this is simpler than we return an InetAddress object which needs to do more
                // work such as serialization
                if (value instanceof String && InetAddresses.isInetAddress(value.toString())) {
                    return value;
                } else {
                    throw new IllegalArgumentException("[" + value + "] is not a valid ipv4/ipv6 address");
                }
            }
        },
        AUTO {
            @Override
            public Object convert(Object value) {
                if (!(value instanceof String)) {
                    return value;
                }
                try {
                    return BOOLEAN.convert(value);
                } catch (IllegalArgumentException e) {}
                try {
                    return INTEGER.convert(value);
                } catch (IllegalArgumentException e) {}
                try {
                    return LONG.convert(value);
                } catch (IllegalArgumentException e) {}
                try {
                    return FLOAT.convert(value);
                } catch (IllegalArgumentException e) {}
                try {
                    return DOUBLE.convert(value);
                } catch (IllegalArgumentException e) {}
                return value;
            }
        };

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }

        public abstract Object convert(Object value);

        public static Type fromString(String processorTag, String propertyName, String type) {
            try {
                return Type.valueOf(type.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw newConfigurationException(
                    TYPE,
                    processorTag,
                    propertyName,
                    "type [" + type + "] not supported, cannot convert field."
                );
            }
        }
    }

    public static final String TYPE = "convert";

    private final String field;
    private final String targetField;
    private final Type convertType;
    private final boolean ignoreMissing;

    ConvertProcessor(String tag, String description, String field, String targetField, Type convertType, boolean ignoreMissing) {
        super(tag, description);
        this.field = field;
        this.targetField = targetField;
        this.convertType = convertType;
        this.ignoreMissing = ignoreMissing;
    }

    String getField() {
        return field;
    }

    String getTargetField() {
        return targetField;
    }

    Type getConvertType() {
        return convertType;
    }

    boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        Object oldValue = document.getFieldValue(field, Object.class, ignoreMissing);
        Object newValue;

        if (oldValue == null && ignoreMissing) {
            return document;
        } else if (oldValue == null) {
            throw new IllegalArgumentException("Field [" + field + "] is null, cannot be converted to type [" + convertType + "]");
        }

        if (oldValue instanceof List) {
            List<?> list = (List<?>) oldValue;
            List<Object> newList = new ArrayList<>(list.size());
            for (Object value : list) {
                newList.add(convertType.convert(value));
            }
            newValue = newList;
        } else {
            newValue = convertType.convert(oldValue);
        }
        document.setFieldValue(targetField, newValue);
        return document;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {
        @Override
        public ConvertProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String typeProperty = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "type");
            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field", field);
            Type convertType = Type.fromString(processorTag, "type", typeProperty);
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            return new ConvertProcessor(processorTag, description, field, targetField, convertType, ignoreMissing);
        }
    }
}
