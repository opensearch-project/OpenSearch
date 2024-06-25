/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query.serializer;

import java.io.IOException;

public interface QuerySearchResultSerializer<T, V> {

    void readQuerySearchResult(T in, boolean isNull) throws IOException;

    void writeQuerySearchResult(V out) throws IOException;

}
