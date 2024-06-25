/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.fetch.subphase.serializer;

import java.io.IOException;

public interface FetchSearchResultSerializer<T, V> {

    void readFetchSearchResult(T in) throws IOException;

    void writeFetchSearchResult(V out) throws IOException;

}
