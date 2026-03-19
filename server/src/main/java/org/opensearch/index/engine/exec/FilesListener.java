/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import java.io.IOException;
import java.util.Collection;

/**
 * Listener for file addition and deletion events.
 *
 * @opensearch.experimental
 */
public interface FilesListener {
    void onFilesDeleted(Collection<String> files) throws IOException;

    void onFilesAdded(Collection<String> files) throws IOException;
}
