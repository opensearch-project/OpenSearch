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

import java.io.IOException;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
@ExperimentalApi()
public class FetchProfileShardResult implements Writeable, ToXContentFragment {
    public static final String FETCH = "fetch";
    public static final String TIME_IN_NANOS = "time_in_nanos";

    private final long fetchTime;

    public FetchProfileShardResult(long fetchTime) {
        this.fetchTime = fetchTime;
    }

    public FetchProfileShardResult(StreamInput in) throws IOException {
        this.fetchTime = in.readLong();
    }

    public long getFetchTime() {
        return fetchTime;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(fetchTime);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject(FETCH).field(TIME_IN_NANOS, fetchTime).endObject();
    }

    public static FetchProfileShardResult fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        String currentFieldName = null;
        long time = 0;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (TIME_IN_NANOS.equals(currentFieldName)) {
                    time = parser.longValue();
                } else {
                    parser.skipChildren();
                }
            } else {
                parser.skipChildren();
            }
        }
        return new FetchProfileShardResult(time);
    }



}
