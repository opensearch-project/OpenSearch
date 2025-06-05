/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import java.io.IOException;

public interface QueryCollectorContextFactory {
    QueryCollectorContext createCollectorContext(CollectorContextParams params) throws IOException;
}
