/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.tiering.status.model;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Response containing tiering status for all indices. */
public class ListTieringStatusResponse extends ActionResponse {

    private List<TieringStatus> tieringStatusList;

    /** Returns the list of tiering statuses. */
    public List<TieringStatus> getTieringStatusList() {
        return tieringStatusList;
    }

    /**
     * Constructs with status list.
     * @param tieringStatusList the status list
     */
    public ListTieringStatusResponse(List<TieringStatus> tieringStatusList) {
        this.tieringStatusList = tieringStatusList;
    }

    /**
     * Constructs from stream.
     * @param in the stream input
     * @throws IOException if error
     */
    public ListTieringStatusResponse(StreamInput in) throws IOException {

        int size = in.readVInt();
        List<TieringStatus> builder = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            builder.add(TieringStatus.readFrom(in));
        }
        tieringStatusList = Collections.unmodifiableList(builder);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(tieringStatusList.size());
        for (TieringStatus tieringStatus : tieringStatusList) {
            tieringStatus.writeTo(out);
        }
    }
}
