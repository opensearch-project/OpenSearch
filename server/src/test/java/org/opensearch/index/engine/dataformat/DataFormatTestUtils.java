/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import java.util.Set;

/**
 * Shared mock implementations for dataformat tests.
 */
public final class DataFormatTestUtils {

    private DataFormatTestUtils() {}

    /**
     * A mock {@link DataFormat} for testing purposes.
     */
    public static class MockDataFormat extends DataFormat {
        @Override
        public String name() {
            return "mock-columnar";
        }

        @Override
        public long priority() {
            return 100L;
        }

        @Override
        public Set<FieldTypeCapabilities> supportedFields() {
            return Set.of(
                new FieldTypeCapabilities(
                    "integer",
                    Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE, FieldTypeCapabilities.Capability.STORED_FIELDS)
                )
            );
        }
    }
}
