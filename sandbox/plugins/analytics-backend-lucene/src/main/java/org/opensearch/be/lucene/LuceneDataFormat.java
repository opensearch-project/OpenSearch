/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability;

import java.util.Set;

/**
 * Lucene data format capabilities. Declares what Lucene provides for each field type:
 * <ul>
 *   <li>keyword — doc values (columnar), inverted index (full-text), stored fields</li>
 *   <li>text — inverted index (full-text), stored fields</li>
 *   <li>integer/long/short/byte — doc values, point range, stored fields</li>
 *   <li>float/double/half_float/scaled_float — doc values, point range, stored fields</li>
 *   <li>date — doc values, point range, stored fields</li>
 *   <li>boolean — doc values, stored fields</li>
 *   <li>ip — doc values, point range, stored fields</li>
 * </ul>
 */
public class LuceneDataFormat extends DataFormat {

    public static final LuceneDataFormat INSTANCE = new LuceneDataFormat();

    private static final Set<Capability> DOC_VALUES_INDEX_STORED = Set.of(
        Capability.COLUMNAR_STORAGE, Capability.FULL_TEXT_SEARCH, Capability.STORED_FIELDS
    );

    private static final Set<Capability> DOC_VALUES_POINT_STORED = Set.of(
        Capability.COLUMNAR_STORAGE, Capability.POINT_RANGE, Capability.STORED_FIELDS
    );

    private static final Set<Capability> FULL_TEXT_STORED = Set.of(
        Capability.FULL_TEXT_SEARCH, Capability.STORED_FIELDS
    );

    private static final Set<Capability> DOC_VALUES_STORED = Set.of(
        Capability.COLUMNAR_STORAGE, Capability.STORED_FIELDS
    );

    private static final Set<FieldTypeCapabilities> SUPPORTED_FIELDS = Set.of(
        // String types
        new FieldTypeCapabilities("keyword", DOC_VALUES_INDEX_STORED),
        new FieldTypeCapabilities("text", FULL_TEXT_STORED),

        // Numeric types — all have doc values + point range
        new FieldTypeCapabilities("integer", DOC_VALUES_POINT_STORED),
        new FieldTypeCapabilities("long", DOC_VALUES_POINT_STORED),
        new FieldTypeCapabilities("short", DOC_VALUES_POINT_STORED),
        new FieldTypeCapabilities("byte", DOC_VALUES_POINT_STORED),
        new FieldTypeCapabilities("float", DOC_VALUES_POINT_STORED),
        new FieldTypeCapabilities("double", DOC_VALUES_POINT_STORED),
        new FieldTypeCapabilities("half_float", DOC_VALUES_POINT_STORED),
        new FieldTypeCapabilities("scaled_float", DOC_VALUES_POINT_STORED),

        // Date
        new FieldTypeCapabilities("date", DOC_VALUES_POINT_STORED),
        new FieldTypeCapabilities("date_nanos", DOC_VALUES_POINT_STORED),

        // Boolean
        new FieldTypeCapabilities("boolean", DOC_VALUES_STORED),

        // IP
        new FieldTypeCapabilities("ip", Set.of(Capability.COLUMNAR_STORAGE, Capability.POINT_RANGE, Capability.STORED_FIELDS))
    );

    @Override
    public String name() {
        return "lucene";
    }

    @Override
    public long priority() {
        return 100;
    }

    @Override
    public Set<FieldTypeCapabilities> supportedFields() {
        return SUPPORTED_FIELDS;
    }
}
