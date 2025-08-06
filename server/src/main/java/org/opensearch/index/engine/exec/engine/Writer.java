/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.engine;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;
import java.util.Optional;

@ExperimentalApi
public interface Writer<P extends DocumentInput<?>> {
    WriteResult addDoc(P d) throws IOException;

    FileMetadata flush() throws IOException;

    void sync() throws IOException;

    void close() throws IOException;

    Optional<FileMetadata> getMetadata();

    P newDocumentInput();
}
