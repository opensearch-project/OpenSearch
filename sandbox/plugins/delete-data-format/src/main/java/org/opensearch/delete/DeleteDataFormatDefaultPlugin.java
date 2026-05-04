/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.delete;

import org.opensearch.delete.engine.DeleteExecutionEngineImpl;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DeleteDataFormatPlugin;
import org.opensearch.index.engine.dataformat.DeleteExecutionEngine;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.plugins.Plugin;

import java.util.Set;

/**
 * Default delete data format plugin that provides {@link DeleteExecutionEngineImpl}
 * for formats that do not handle deletes natively.
 *
 * @opensearch.experimental
 */
public class DeleteDataFormatDefaultPlugin extends Plugin implements DeleteDataFormatPlugin {

    private static final DataFormat DELETE_FORMAT = new DataFormat() {
        @Override
        public String name() {
            return "delete";
        }

        @Override
        public long priority() {
            return 0;
        }

        @Override
        public Set<FieldTypeCapabilities> supportedFields() {
            return Set.of();
        }
    };

    @Override
    public DataFormat getDataFormat() {
        return DELETE_FORMAT;
    }

    @Override
    public DeleteExecutionEngine<?> deleteEngine() {
        return new DeleteExecutionEngineImpl(DELETE_FORMAT);
    }
}
