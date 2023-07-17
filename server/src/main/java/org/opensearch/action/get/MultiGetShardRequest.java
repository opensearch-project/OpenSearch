/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.get;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.single.shard.SingleShardRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Multi get shards.
 *
 * @opensearch.internal
 */
public class MultiGetShardRequest extends SingleShardRequest<MultiGetShardRequest> {

    private int shardId;
    private String preference;
    private boolean realtime;
    private boolean refresh;

    List<Integer> locations;
    List<MultiGetRequest.Item> items;

    MultiGetShardRequest(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        locations = new ArrayList<>(size);
        items = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            locations.add(in.readVInt());
            items.add(new MultiGetRequest.Item(in));
        }

        preference = in.readOptionalString();
        refresh = in.readBoolean();
        realtime = in.readBoolean();
    }

    MultiGetShardRequest(MultiGetRequest multiGetRequest, String index, int shardId) {
        super(index);
        this.shardId = shardId;
        locations = new ArrayList<>();
        items = new ArrayList<>();
        preference = multiGetRequest.preference;
        realtime = multiGetRequest.realtime;
        refresh = multiGetRequest.refresh;
    }

    @Override
    public ActionRequestValidationException validate() {
        return super.validateNonNullIndex();
    }

    public int shardId() {
        return this.shardId;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * {@code _local} to prefer local shards, {@code _primary} to execute only on primary shards,
     * or a custom value, which guarantees that the same order
     * will be used across different requests.
     */
    public MultiGetShardRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    public String preference() {
        return this.preference;
    }

    public boolean realtime() {
        return this.realtime;
    }

    public MultiGetShardRequest realtime(boolean realtime) {
        this.realtime = realtime;
        return this;
    }

    public boolean refresh() {
        return this.refresh;
    }

    public MultiGetShardRequest refresh(boolean refresh) {
        this.refresh = refresh;
        return this;
    }

    void add(int location, MultiGetRequest.Item item) {
        this.locations.add(location);
        this.items.add(item);
    }

    @Override
    public String[] indices() {
        String[] indices = new String[items.size()];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = items.get(i).index();
        }
        return indices;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(locations.size());

        for (int i = 0; i < locations.size(); i++) {
            out.writeVInt(locations.get(i));
            items.get(i).writeTo(out);
        }

        out.writeOptionalString(preference);
        out.writeBoolean(refresh);
        out.writeBoolean(realtime);
    }
}
