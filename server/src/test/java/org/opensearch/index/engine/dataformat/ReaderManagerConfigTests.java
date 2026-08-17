/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.index.engine.dataformat.stub.MockDataFormat;
import org.opensearch.plugins.NativeStoreHandle;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ReaderManagerConfigTests extends OpenSearchTestCase {

    private static final MockDataFormat PARQUET = new MockDataFormat(
        "parquet",
        100L,
        Set.of(new FieldTypeCapabilities("integer", Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE)))
    );

    private static final MockDataFormat LUCENE = new MockDataFormat(
        "lucene",
        50L,
        Set.of(new FieldTypeCapabilities("keyword", Set.of(FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH)))
    );

    private static ReaderManagerConfig configFor(DataFormat format, Map<DataFormat, NativeStoreHandle> handles) {
        return new ReaderManagerConfig(Optional.empty(), format, null, null, handles, null);
    }

    public void testResolvesTheFormatsOwnHandle() {
        NativeStoreHandle parquetHandle = new NativeStoreHandle(1L, ptr -> {});
        try {
            assertSame(parquetHandle, configFor(PARQUET, Map.of(PARQUET, parquetHandle)).storeHandle());
        } finally {
            parquetHandle.close();
        }
    }

    public void testAuxiliaryFormatResolvesToItsDelegatesHandle() {
        // The side table's files are physically the delegate's — same directory, same native file
        // registry — so the handle that applies to them is the delegate's.
        AuxiliaryDataFormat child = new AuxiliaryDataFormat(PARQUET, AuxiliaryDataFormat.NESTED_CHILD_ROLE);
        NativeStoreHandle parquetHandle = new NativeStoreHandle(1L, ptr -> {});
        NativeStoreHandle luceneHandle = new NativeStoreHandle(2L, ptr -> {});
        try {
            ReaderManagerConfig config = configFor(child, Map.of(PARQUET, parquetHandle, LUCENE, luceneHandle));

            assertSame(parquetHandle, config.storeHandle());
        } finally {
            parquetHandle.close();
            luceneHandle.close();
        }
    }

    public void testAuxiliaryFormatIgnoresAHandleRegisteredUnderItsOwnName() {
        // A handle keyed on the side table's *catalog* name would name a native file registry that
        // nothing populates: StoreStrategy#owns matches on the path prefix, and the child's files are
        // at `parquet/…`, so only the parquet strategy ever claims them. A live-but-empty registry is
        // worse than none, because the reader would take the with-store path against it instead of
        // falling back to the local file system.
        AuxiliaryDataFormat child = new AuxiliaryDataFormat(PARQUET, AuxiliaryDataFormat.NESTED_CHILD_ROLE);
        NativeStoreHandle parquetHandle = new NativeStoreHandle(1L, ptr -> {});
        NativeStoreHandle strayChildHandle = new NativeStoreHandle(2L, ptr -> {});
        try {
            ReaderManagerConfig config = configFor(child, Map.of(PARQUET, parquetHandle, child, strayChildHandle));

            assertSame(parquetHandle, config.storeHandle());
        } finally {
            parquetHandle.close();
            strayChildHandle.close();
        }
    }

    public void testMissingHandleIsNullSoCallersFallBackToLocalFs() {
        assertNull(configFor(PARQUET, Map.of()).storeHandle());
        assertNull(
            "an auxiliary format whose delegate has no handle must also report none",
            configFor(new AuxiliaryDataFormat(PARQUET, AuxiliaryDataFormat.NESTED_CHILD_ROLE), Map.of()).storeHandle()
        );
    }
}
