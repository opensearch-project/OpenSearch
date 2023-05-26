/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Pit ID along with Id for a search context per node.
 *
 * @opensearch.internal
 */
public class PitSearchContextIdForNode implements Writeable {

    private final String pitId;
    private final SearchContextIdForNode searchContextIdForNode;

    public PitSearchContextIdForNode(String pitId, SearchContextIdForNode searchContextIdForNode) {
        this.pitId = pitId;
        this.searchContextIdForNode = searchContextIdForNode;
    }

    PitSearchContextIdForNode(StreamInput in) throws IOException {
        this.pitId = in.readString();
        this.searchContextIdForNode = new SearchContextIdForNode(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(pitId);
        searchContextIdForNode.writeTo(out);
    }

    public String getPitId() {
        return pitId;
    }

    public SearchContextIdForNode getSearchContextIdForNode() {
        return searchContextIdForNode;
    }
}
