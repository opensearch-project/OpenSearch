/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.aggregation.support;

import org.opensearch.script.Script;

/**
 * Wrapper for common ValuesSource aggregation fields extracted from proto messages.
 * Reduces parameter count in {@link ValuesSourceAggregationProtoUtils#declareFields}.
 */
public class ValuesSourceProtoFields {
    private final String field;
    private final String format;
    private final Object missing;
    private final Script script;
    private final Enum<?> valueTypeProto;

    private ValuesSourceProtoFields(Builder builder) {
        this.field = builder.field;
        this.format = builder.format;
        this.missing = builder.missing;
        this.script = builder.script;
        this.valueTypeProto = builder.valueTypeProto;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getField() {
        return field;
    }

    public String getFormat() {
        return format;
    }

    public Object getMissing() {
        return missing;
    }

    public Script getScript() {
        return script;
    }

    public Enum<?> getValueTypeProto() {
        return valueTypeProto;
    }

    /**
     * Builder for {@link ValuesSourceProtoFields}.
     */
    public static class Builder {
        private String field;
        private String format;
        private Object missing;
        private Script script;
        private Enum<?> valueTypeProto;

        private Builder() {}

        public Builder field(String field) {
            this.field = field;
            return this;
        }

        public Builder format(String format) {
            this.format = format;
            return this;
        }

        public Builder missing(Object missing) {
            this.missing = missing;
            return this;
        }

        public Builder script(Script script) {
            this.script = script;
            return this;
        }

        public Builder valueType(Enum<?> valueTypeProto) {
            this.valueTypeProto = valueTypeProto;
            return this;
        }

        public ValuesSourceProtoFields build() {
            return new ValuesSourceProtoFields(this);
        }
    }
}
