/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search;

import java.util.Iterator;

// Interface for the iterator that Datafusion expects
public interface SearchResultIterator extends Iterator<Record> {
    // Basic Iterator methods
    boolean hasNext();
    Record next();
}
