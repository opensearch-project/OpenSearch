/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import java.io.IOException;
import java.util.Optional;

public interface Writer<P extends DocumentInput<?>> {
    WriteResult addDoc(P d) throws IOException;

    FileMetadata flush(FlushIn flushIn) throws IOException;

    void sync() throws IOException;

    void close();

    Optional<FileMetadata> getMetadata();

    P newDocumentInput();
}
