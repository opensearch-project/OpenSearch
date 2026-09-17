/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.apache.lucene.index.DirectoryReader;
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Bridge that lets core obtain a Lucene {@link DirectoryReader} from a data-format-specific reader
 * (for example, the Lucene secondary of a composite index) without depending on the format plugin
 * that produces it. Format readers that are backed by Lucene implement this interface so that
 * shard-level Lucene operations — such as the Explain API — can run against the Lucene copy while
 * the primary data lives in another format (e.g. Parquet).
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface LuceneReaderAccess {

    /**
     * Returns the underlying Lucene {@link DirectoryReader} backing this format reader.
     *
     * @return the directory reader, or {@code null} if none is available
     */
    DirectoryReader directoryReader();
}
