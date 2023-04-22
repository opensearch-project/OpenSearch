/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.rules.action;

import org.opensearch.action.ActionResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.correlation.rules.model.CorrelationRule;
import org.opensearch.rest.RestStatus;

import java.io.IOException;

/**
 * Transport Response for indexing correlation rules.
 *
 * @opensearch.internal
 */
public class IndexCorrelationRuleResponse extends ActionResponse implements ToXContentObject {

    private static final ParseField _ID = new ParseField("_id");
    private static final ParseField _VERSION = new ParseField("_version");

    private String id;

    private Long version;

    private RestStatus status;

    private CorrelationRule correlationRule;

    /**
     * Parameterized ctor for IndexCorrelationRuleResponse
     * @param version version of rule
     * @param status Rest status of indexing rule
     * @param correlationRule correlation rule
     */
    public IndexCorrelationRuleResponse(String id, Long version, RestStatus status, CorrelationRule correlationRule) {
        super();
        this.id = id;
        this.version = version;
        this.status = status;
        this.correlationRule = correlationRule;
    }

    /**
     * StreamInput ctor of IndexCorrelationRuleResponse
     * @param sin StreamInput
     * @throws IOException IOException
     */
    public IndexCorrelationRuleResponse(StreamInput sin) throws IOException {
        this(sin.readString(), sin.readLong(), sin.readEnum(RestStatus.class), CorrelationRule.readFrom(sin));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().field(_ID.getPreferredName(), id).field(_VERSION.getPreferredName(), version);

        builder.field("rule", correlationRule);
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeLong(version);
        out.writeEnum(status);
        correlationRule.writeTo(out);
    }

    /**
     * get id
     * @return id of rule
     */
    public String getId() {
        return id;
    }

    /**
     * get status
     * @return Rest status of indexing rule
     */
    public RestStatus getStatus() {
        return status;
    }
}
