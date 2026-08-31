/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.stub;

import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability.COLUMNAR_STORAGE;

/**
 * A mock data format plugin whose format backs only <b>columnar storage</b> for doc-values-backed field
 * types (numeric, date, ip, boolean, scaled_float) — it declares no {@code POINT_RANGE} /
 * {@code FULL_TEXT_SEARCH} for them, mirroring the real pluggable data format.
 * <p>
 * This class only <i>declares</i> capabilities. Rejecting {@code index:true} (a request for a search
 * capability the format does not back) is done by the core capability path
 * ({@link org.opensearch.index.engine.dataformat.DataFormatPlugin#assignCapabilities}), not here — so
 * there is deliberately no exception thrown in this mock.
 * <p>
 * It reuses {@link MockParquetDataFormatPlugin}'s metadata/text coverage so that system fields resolve
 * during mapping build, and adds the doc-values-backed value types with {@code COLUMNAR_STORAGE} only.
 */
public class MockDocValuesDataFormatPlugin extends MockDataFormatPlugin {

    /** Format name to set in {@code index.pluggable.dataformat}. */
    public static final String FORMAT_NAME = "mock-dv";

    private static final List<String> DOC_VALUES_BACKED_TYPES = List.of(
        "byte",
        "short",
        "integer",
        "long",
        "float",
        "double",
        "half_float",
        "unsigned_long",
        "date",
        "date_nanos",
        "ip",
        "boolean",
        "scaled_float"
    );

    public MockDocValuesDataFormatPlugin() {
        super(new MockDataFormat(FORMAT_NAME, 100L, columnarStorageFields()));
    }

    private static Set<FieldTypeCapabilities> columnarStorageFields() {
        // Reuse the parquet mock's metadata + text coverage so system/metadata fields resolve during build,
        // then add each doc-values-backed value type with columnar storage only (no search capability).
        Set<FieldTypeCapabilities> fields = new HashSet<>(new MockParquetDataFormatPlugin().getDataFormat().supportedFields());
        for (String type : DOC_VALUES_BACKED_TYPES) {
            fields.add(new FieldTypeCapabilities(type, Set.of(COLUMNAR_STORAGE)));
        }
        return Set.copyOf(fields);
    }
}
