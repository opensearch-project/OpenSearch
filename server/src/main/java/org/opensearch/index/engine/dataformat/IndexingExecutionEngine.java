/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * @opensearch.experimental
 */
@ExperimentalApi
public interface IndexingExecutionEngine<T extends DataFormat, P extends DocumentInput<?>> {

    Writer<P> createWriter(long writerGeneration) throws IOException;

    Merger getMerger();

    RefreshResult refresh(RefreshInput refreshInput) throws IOException;

    T getDataFormat();

    void deleteFiles(Map<String, Collection<String>> filesToDelete) throws IOException;

    P newDocumentInput();

    default long getNativeBytesUsed() {
        return 0;
    }
}
