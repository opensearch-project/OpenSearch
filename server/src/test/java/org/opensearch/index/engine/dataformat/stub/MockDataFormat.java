/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.stub;

import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;

import java.util.Set;

/**
 * A mock {@link DataFormat} for testing purposes.
 */
public class MockDataFormat extends DataFormat {

    private final String name;
    private final long priority;
    private final Set<FieldTypeCapabilities> supportedFields;

    public MockDataFormat() {
        this(
            "mock-columnar",
            100L,
            Set.of(
                new FieldTypeCapabilities(
                    "integer",
                    Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE, FieldTypeCapabilities.Capability.STORED_FIELDS)
                )
            )
        );
    }

    public MockDataFormat(String name, long priority, Set<FieldTypeCapabilities> supportedFields) {
        this.name = name;
        this.priority = priority;
        this.supportedFields = supportedFields;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public long priority() {
        return priority;
    }

    @Override
    public Set<FieldTypeCapabilities> supportedFields() {
        return supportedFields;
    }
}
