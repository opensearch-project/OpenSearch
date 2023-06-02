/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.rules.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.plugin.correlation.rules.model.CorrelationRule;
import org.opensearch.rest.RestRequest;

import java.io.IOException;

/**
 * A request to index correlation rules.
 *
 * @opensearch.internal
 */
public class IndexCorrelationRuleRequest extends ActionRequest {

    private String correlationRuleId;

    private CorrelationRule correlationRule;

    private RestRequest.Method method;

    /**
     * Parameterized ctor for IndexCorrelationRuleRequest
     * @param correlationRule correlation rule
     * @param method Rest method of request PUT or POST
     */
    public IndexCorrelationRuleRequest(CorrelationRule correlationRule, RestRequest.Method method) {
        super();
        this.correlationRuleId = "";
        this.correlationRule = correlationRule;
        this.method = method;
    }

    /**
     * Parameterized ctor for IndexCorrelationRuleRequest
     * @param correlationRuleId correlation rule id
     * @param correlationRule correlation rule
     * @param method Rest method of request PUT or POST
     */
    public IndexCorrelationRuleRequest(String correlationRuleId, CorrelationRule correlationRule, RestRequest.Method method) {
        super();
        this.correlationRuleId = correlationRuleId;
        this.correlationRule = correlationRule;
        this.method = method;
    }

    /**
     * StreamInput ctor of IndexCorrelationRuleRequest
     * @param sin StreamInput
     * @throws IOException IOException
     */
    public IndexCorrelationRuleRequest(StreamInput sin) throws IOException {
        this(sin.readString(), CorrelationRule.readFrom(sin), sin.readEnum(RestRequest.Method.class));
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(correlationRuleId);
        correlationRule.writeTo(out);
    }

    /**
     * get correlation rule id
     * @return correlation rule id
     */
    public String getCorrelationRuleId() {
        return correlationRuleId;
    }

    /**
     * get correlation rule
     * @return correlation rule
     */
    public CorrelationRule getCorrelationRule() {
        return correlationRule;
    }

    /**
     * get Rest method
     * @return Rest method
     */
    public RestRequest.Method getMethod() {
        return method;
    }
}
