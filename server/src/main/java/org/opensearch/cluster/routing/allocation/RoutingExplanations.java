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

package org.opensearch.cluster.routing.allocation;

import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Class used to encapsulate a number of {@link RerouteExplanation}
 * explanations.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class RoutingExplanations implements ToXContentFragment {
    private final List<RerouteExplanation> explanations;

    public RoutingExplanations() {
        this.explanations = new ArrayList<>();
    }

    public RoutingExplanations add(RerouteExplanation explanation) {
        this.explanations.add(explanation);
        return this;
    }

    public List<RerouteExplanation> explanations() {
        return this.explanations;
    }

    /**
     * Provides feedback from commands with a YES decision that should be displayed to the user after the command has been applied
     */
    public List<String> getYesDecisionMessages() {
        return explanations().stream()
            .filter(explanation -> explanation.decisions().type().equals(Decision.Type.YES))
            .map(explanation -> explanation.command().getMessage())
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());
    }

    /**
     * Read in a RoutingExplanations object
     */
    public static RoutingExplanations readFrom(StreamInput in) throws IOException {
        int exCount = in.readVInt();
        RoutingExplanations exp = new RoutingExplanations();
        for (int i = 0; i < exCount; i++) {
            RerouteExplanation explanation = RerouteExplanation.readFrom(in);
            exp.add(explanation);
        }
        return exp;
    }

    /**
     * Write the RoutingExplanations object
     */
    public static void writeTo(RoutingExplanations explanations, StreamOutput out) throws IOException {
        out.writeVInt(explanations.explanations.size());
        for (RerouteExplanation explanation : explanations.explanations) {
            RerouteExplanation.writeTo(explanation, out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("explanations");
        for (RerouteExplanation explanation : explanations) {
            explanation.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }
}
