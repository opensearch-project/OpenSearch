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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.action.fieldcaps;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Describes the capabilities of a field in a single index.
 *
 * @opensearch.internal
 */
public class IndexFieldCapabilities implements Writeable {

    private final String name;
    private final String type;
    private final boolean isAlias;
    private final boolean isSearchable;
    private final boolean isAggregatable;
    private final List<String> aliases;
    private final Map<String, String> meta;

    /**
     * @param name The name of the field.
     * @param type The type associated with the field.
     * @param isSearchable Whether this field is indexed for search.
     * @param isAggregatable Whether this field can be aggregated on.
     * @param meta Metadata about the field.
     */
    IndexFieldCapabilities(
        String name,
        String type,
        boolean isAlias,
        boolean isSearchable,
        boolean isAggregatable,
        List<String> aliases,
        Map<String, String> meta
    ) {

        this.name = name;
        this.type = type;
        this.isAlias = isAlias;
        this.isSearchable = isSearchable;
        this.isAggregatable = isAggregatable;
        this.aliases = aliases;
        this.meta = meta;
    }

    IndexFieldCapabilities(StreamInput in) throws IOException {
        this.name = in.readString();
        this.type = in.readString();
        this.isAlias = in.readBoolean();
        this.isSearchable = in.readBoolean();
        this.isAggregatable = in.readBoolean();
        this.aliases = in.readOptionalStringList();
        this.meta = in.readMap(StreamInput::readString, StreamInput::readString);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(type);
        out.writeBoolean(isAlias);
        out.writeBoolean(isSearchable);
        out.writeBoolean(isAggregatable);
        out.writeOptionalStringCollection(aliases);
        out.writeMap(meta, StreamOutput::writeString, StreamOutput::writeString);
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public boolean isAlias() {
        return isAlias;
    }

    public boolean isAggregatable() {
        return isAggregatable;
    }

    public boolean isSearchable() {
        return isSearchable;
    }

    public List<String> aliases() {
        return aliases;
    }

    public Map<String, String> meta() {
        return meta;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexFieldCapabilities that = (IndexFieldCapabilities) o;
        return isAlias == that.isAlias
            && isSearchable == that.isSearchable
            && isAggregatable == that.isAggregatable
            && Objects.equals(name, that.name)
            && Objects.equals(type, that.type)
            && Objects.equals(aliases, that.aliases)
            && Objects.equals(meta, that.meta);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, isAlias, isSearchable, isAggregatable, aliases, meta);
    }
}
