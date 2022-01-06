/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sort;

import org.opensearch.common.xcontent.XContentParser;

import java.io.IOException;

@FunctionalInterface
public interface SortParser<SB extends SortBuilder<SB>> {
    /**
     * Creates a new {@link SortBuilder} from the sort held by the
     * {@link XContentParser}. The state on the parser contained in this context
     * will be changed as a side effect of this method call
     */
    SB fromXContent(XContentParser parser, String elementName) throws IOException;
}
