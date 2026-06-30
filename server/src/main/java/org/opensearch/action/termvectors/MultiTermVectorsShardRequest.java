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

package org.opensearch.action.termvectors;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.single.shard.SingleShardRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A multi get shard request.
 *
 * @opensearch.internal
 */
public class MultiTermVectorsShardRequest extends SingleShardRequest<MultiTermVectorsShardRequest> {

    private int shardId;
    private String preference;

    List<Integer> locations;
    List<TermVectorsRequest> requests;

    MultiTermVectorsShardRequest(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        locations = new ArrayList<>(size);
        requests = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            locations.add(in.readVInt());
            requests.add(new TermVectorsRequest(in));
        }

        preference = in.readOptionalString();
    }

    MultiTermVectorsShardRequest(String index, int shardId) {
        super(index);
        this.shardId = shardId;
        locations = new ArrayList<>();
        requests = new ArrayList<>();
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
    public MultiTermVectorsShardRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    public String preference() {
        return this.preference;
    }

    public void add(int location, TermVectorsRequest request) {
        this.locations.add(location);
        this.requests.add(request);
    }

    @Override
    public String[] indices() {
        String[] indices = new String[requests.size()];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = requests.get(i).index();
        }
        return indices;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(locations.size());
        for (int i = 0; i < locations.size(); i++) {
            out.writeVInt(locations.get(i));
            requests.get(i).writeTo(out);
        }

        out.writeOptionalString(preference);
    }
}
