/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.*;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Profile shard level result that corresponds to a {@link ProfileResult}
 *
 * @opensearch.api
 */
@PublicApi(since = "3.0.0")
public class AbstractProfileShardResult implements Writeable, ToXContentObject {

    public static final String RESULTS_ARRAY = "results";

    protected final List<ProfileResult> profileResults;

    public AbstractProfileShardResult(List<ProfileResult> profileResults) {
        this.profileResults = profileResults;
    }

    public AbstractProfileShardResult(StreamInput in) throws IOException {
        int profileSize = in.readVInt();
        profileResults = new ArrayList<>(profileSize);
        for (int j = 0; j < profileSize; j++) {
            profileResults.add(new ProfileResult(in));
        }
    }

    public List<ProfileResult> getProfileResults() {
        return Collections.unmodifiableList(profileResults);
    }

    public static AbstractProfileShardResult fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        String currentFieldName = null;
        List<ProfileResult> profileResults = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                parser.skipChildren();
            } else if (token == XContentParser.Token.START_ARRAY) {
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    profileResults.add(ProfileResult.fromXContent(parser));
                }
            } else {
                parser.skipChildren();
            }
        }
        return new AbstractProfileShardResult(profileResults);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(profileResults.size());
        for (ProfileResult p : profileResults) {
            p.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray(RESULTS_ARRAY);
        for (ProfileResult p : profileResults) {
            p.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }
}
