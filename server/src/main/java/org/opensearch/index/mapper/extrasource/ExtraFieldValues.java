/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.extrasource;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

@ExperimentalApi()
public final class ExtraFieldValues implements Writeable {

    public static final ExtraFieldValues EMPTY = new ExtraFieldValues(Map.of());

    private final Map<String, ExtraFieldValue> values; // fieldPath -> value

    public ExtraFieldValues(Map<String, ExtraFieldValue> values) {
        this.values = Map.copyOf(values);
    }

    public ExtraFieldValues(StreamInput in) throws IOException {
        final Map<String, ExtraFieldValue> m = in.readMap(StreamInput::readString, ExtraFieldValue::readFrom);
        this.values = m.isEmpty() ? Map.of() : Collections.unmodifiableMap(m);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(values, StreamOutput::writeString, (o, v) -> v.writeTo(o));
    }

    public boolean isEmpty() {
        return values.isEmpty();
    }

    public Map<String, ExtraFieldValue> values() {
        return values;
    }

    public ExtraFieldValue get(String path) {
        return values.get(path);
    }

}
