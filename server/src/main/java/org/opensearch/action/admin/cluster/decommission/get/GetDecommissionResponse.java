/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.decommission.get;

import org.opensearch.action.ActionResponse;
import org.opensearch.cluster.metadata.DecommissionAttributeMetadata;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class GetDecommissionResponse extends ActionResponse implements ToXContentObject {

    private DecommissionAttributeMetadata decommissionedAttribute;

    GetDecommissionResponse(DecommissionAttributeMetadata decommissionedAttribute) {
        this.decommissionedAttribute = decommissionedAttribute;
    }

    GetDecommissionResponse(StreamInput in) throws IOException {
        decommissionedAttribute = new DecommissionAttributeMetadata(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        decommissionedAttribute.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        decommissionedAttribute.toXContent(
                builder,
                null //TODO - check for params here if any
        );
        builder.endObject();
        return builder;
    }

    public static GetDecommissionResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        return new GetDecommissionResponse(DecommissionAttributeMetadata.fromXContent(parser));
    }
}