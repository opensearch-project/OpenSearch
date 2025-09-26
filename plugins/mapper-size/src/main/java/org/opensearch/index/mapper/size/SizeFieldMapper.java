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

package org.opensearch.index.mapper.size;

import org.opensearch.common.Explicit;
import org.opensearch.index.mapper.FieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MetadataFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper.NumberFieldType;
import org.opensearch.index.mapper.NumberFieldMapper.NumberType;
import org.opensearch.index.mapper.ParametrizedFieldMapper;
import org.opensearch.index.mapper.ParseContext;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class SizeFieldMapper extends MetadataFieldMapper {
    public static final String NAME = "_size";

    private static SizeFieldMapper toType(FieldMapper in) {
        return (SizeFieldMapper) in;
    }

    public static class Builder extends MetadataFieldMapper.Builder {

        private final Parameter<Explicit<Boolean>> enabled = updateableBoolParam("enabled", m -> toType(m).enabled, false);

        private Builder() {
            super(NAME);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Collections.singletonList(enabled);
        }

        @Override
        public SizeFieldMapper build(BuilderContext context) {
            return new SizeFieldMapper(enabled.getValue(), new NumberFieldType(NAME, NumberType.INTEGER));
        }
    }

    public static final TypeParser PARSER = new ConfigurableTypeParser(
        c -> new SizeFieldMapper(new Explicit<>(false, false), new NumberFieldType(NAME, NumberType.INTEGER)),
        c -> new Builder()
    );

    private final Explicit<Boolean> enabled;

    private SizeFieldMapper(Explicit<Boolean> enabled, MappedFieldType mappedFieldType) {
        super(mappedFieldType);
        this.enabled = enabled;
    }

    @Override
    protected String contentType() {
        return NAME;
    }

    public boolean enabled() {
        return this.enabled.value();
    }

    @Override
    public void postParse(ParseContext context) throws IOException {
        // we post parse it so we get the size stored, possibly compressed (source will be preParse)
        if (enabled.value() == false) {
            return;
        }
        final int value = context.sourceToParse().source().length();
        context.doc().addAll(NumberType.INTEGER.createFields(name(), value, true, true, false, true));
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new Builder().init(this);
    }
}
