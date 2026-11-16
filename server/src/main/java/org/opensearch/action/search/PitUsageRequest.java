/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.broadcast.BroadcastRequest;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Transport request for retrieving PITs usage information
 *
 * @opensearch.api
 */
@PublicApi(since = "3.11.0")
public class PitUsageRequest extends BroadcastRequest<PitUsageRequest> {
    private final List<String> pitIds = new ArrayList<>();

    public PitUsageRequest() {
        this(Strings.EMPTY_ARRAY);
    }

    public PitUsageRequest(StreamInput in) throws IOException {
        super(in);
        pitIds.addAll(Arrays.asList(in.readStringArray()));
    }

    public PitUsageRequest(String... pitIds) {
        super(Strings.EMPTY_ARRAY);
        this.pitIds.addAll(Arrays.asList(pitIds));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(pitIds.toArray(new String[0]));
    }

    public List<String> getPitIds() {
        return Collections.unmodifiableList(pitIds);
    }

    public void clearAndSetPitIds(List<String> pitIds) {
        this.pitIds.clear();
        this.pitIds.addAll(pitIds);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (pitIds.isEmpty()) {
            validationException = addValidationError("no pit ids specified", validationException);
        }
        return validationException;
    }

    public void fromXContent(XContentParser parser) throws IOException {
        pitIds.clear();
        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("Malformed content, must start with an object");
        } else {
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if ("pit_id".equals(currentFieldName)) {
                    if (token == XContentParser.Token.START_ARRAY) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (token.isValue() == false) {
                                throw new IllegalArgumentException("pit_id array element should only contain PIT identifier");
                            }
                            pitIds.add(parser.text());
                        }
                    } else {
                        if (token.isValue() == false) {
                            throw new IllegalArgumentException("pit_id element should only contain PIT identifier");
                        }
                        pitIds.add(parser.text());
                    }
                } else {
                    throw new IllegalArgumentException(
                        "Unknown parameter [" + currentFieldName + "] in request body or parameter is of the wrong type[" + token + "] "
                    );
                }
            }
        }
    }
}
