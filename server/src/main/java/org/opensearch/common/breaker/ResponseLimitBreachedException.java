/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.breaker;

import org.opensearch.OpenSearchException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Thrown when api response breaches threshold limit.
 *
 * @opensearch.internal
 */
public class ResponseLimitBreachedException extends OpenSearchException {

    private final int responseLimit;
    private final ResponseLimitSettings.LimitEntity limitEntity;

    public ResponseLimitBreachedException(StreamInput in) throws IOException {
        super(in);
        responseLimit = in.readVInt();
        limitEntity = in.readEnum(ResponseLimitSettings.LimitEntity.class);
    }

    public ResponseLimitBreachedException(String msg, int responseLimit, ResponseLimitSettings.LimitEntity limitEntity) {
        super(msg);
        this.responseLimit = responseLimit;
        this.limitEntity = limitEntity;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(responseLimit);
        out.writeEnum(limitEntity);
    }

    public int getResponseLimit() {
        return responseLimit;
    }

    public ResponseLimitSettings.LimitEntity getLimitEntity() {
        return limitEntity;
    }

    @Override
    public RestStatus status() {
        return RestStatus.TOO_MANY_REQUESTS;
    }

    @Override
    protected void metadataToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("response_limit", responseLimit);
        builder.field("limit_entity", limitEntity);
    }
}
