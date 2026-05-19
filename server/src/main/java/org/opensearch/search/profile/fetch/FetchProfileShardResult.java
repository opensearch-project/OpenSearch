/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.fetch;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.profile.ProfileResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Represents the fetch phase profile results for a single shard.
 */
@ExperimentalApi()
public class FetchProfileShardResult implements Writeable, ToXContentFragment {
    public static final String FETCH = "fetch";

    private final List<ProfileResult> fetchProfileResults;

    public FetchProfileShardResult(List<ProfileResult> results) {
        this.fetchProfileResults = Collections.unmodifiableList(results);
    }

    public FetchProfileShardResult(StreamInput in) throws IOException {
        int profileSize = in.readVInt();
        List<ProfileResult> tmp = new ArrayList<>(profileSize);
        for (int j = 0; j < profileSize; j++) {
            tmp.add(new ProfileResult(in));
        }
        this.fetchProfileResults = Collections.unmodifiableList(tmp);
    }

    public List<ProfileResult> getFetchProfileResults() {
        return fetchProfileResults;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(fetchProfileResults.size());
        for (ProfileResult p : fetchProfileResults) {
            p.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(FETCH);
        for (ProfileResult p : fetchProfileResults) {
            p.toXContent(builder, params);
        }
        return builder.endArray();
    }

    public static FetchProfileShardResult fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
        List<ProfileResult> results = new ArrayList<>();
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            results.add(ProfileResult.fromXContent(parser));
        }
        return new FetchProfileShardResult(results);
    }
}
