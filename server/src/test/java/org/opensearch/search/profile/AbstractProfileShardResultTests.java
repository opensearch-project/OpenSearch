/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AbstractProfileShardResultTests extends OpenSearchTestCase {

    public static AbstractProfileShardResult<?> createTestItem() {
        int size = randomIntBetween(0, 5);
        List<TimingProfileResult> profileResults = new ArrayList<>(size);
        for(int i = 0; i < size; i++) {
            profileResults.add(TimingProfileResultTests.createTestItem(1, false));
        }

        return new AbstractProfileShardResult<TimingProfileResult>(profileResults) {
            @Override
            public TimingProfileResult createProfileResult(StreamInput in) throws IOException {
                return new TimingProfileResult(in);
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeVInt(profileResults.size());
                for (TimingProfileResult p : profileResults) {
                    p.writeTo(out);
                }
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.startArray("test-plugin");
                for (TimingProfileResult p : profileResults) {
                    p.toXContent(builder, params);
                }
                builder.endArray();
                builder.endObject();
                return builder;
            }
        };
    }

}
