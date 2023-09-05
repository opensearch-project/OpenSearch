/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.decommission.awareness.put;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.cluster.decommission.DecommissionAttribute;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.Strings;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Registers a decommission request with decommission attribute and timeout
 *
 * @opensearch.internal
 */
public class DecommissionRequest extends ClusterManagerNodeRequest<DecommissionRequest> {

    public static final TimeValue DEFAULT_NODE_DRAINING_TIMEOUT = TimeValue.timeValueSeconds(120);

    private DecommissionAttribute decommissionAttribute;
    private String requestID;
    private TimeValue delayTimeout = DEFAULT_NODE_DRAINING_TIMEOUT;

    // holder for no_delay param. To avoid draining time timeout.
    private boolean noDelay = false;

    public DecommissionRequest() {}

    public DecommissionRequest(DecommissionAttribute decommissionAttribute) {
        this.decommissionAttribute = decommissionAttribute;
    }

    public DecommissionRequest(StreamInput in) throws IOException {
        super(in);
        decommissionAttribute = new DecommissionAttribute(in);
        this.delayTimeout = in.readTimeValue();
        this.noDelay = in.readBoolean();
        this.requestID = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        decommissionAttribute.writeTo(out);
        out.writeTimeValue(delayTimeout);
        out.writeBoolean(noDelay);
        out.writeOptionalString(requestID);
    }

    /**
     * Sets decommission attribute for decommission request
     *
     * @param decommissionAttribute attribute key-value that needs to be decommissioned
     * @return this request
     */
    public DecommissionRequest setDecommissionAttribute(DecommissionAttribute decommissionAttribute) {
        this.decommissionAttribute = decommissionAttribute;
        return this;
    }

    /**
     * @return Returns the decommission attribute key-value
     */
    public DecommissionAttribute getDecommissionAttribute() {
        return this.decommissionAttribute;
    }

    public DecommissionRequest setDelayTimeout(TimeValue delayTimeout) {
        this.delayTimeout = delayTimeout;
        return this;
    }

    public TimeValue getDelayTimeout() {
        return this.delayTimeout;
    }

    public DecommissionRequest setNoDelay(boolean noDelay) {
        if (noDelay) {
            this.delayTimeout = TimeValue.ZERO;
        }
        this.noDelay = noDelay;
        return this;
    }

    public boolean isNoDelay() {
        return noDelay;
    }

    /**
     * Sets id for decommission request
     *
     * @param requestID uuid for request
     * @return this request
     */
    public DecommissionRequest setRequestID(String requestID) {
        this.requestID = requestID;
        return this;
    }

    /**
     * @return Returns id of decommission request
     */
    public String requestID() {
        return requestID;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (decommissionAttribute == null) {
            validationException = addValidationError("decommission attribute is missing", validationException);
            return validationException;
        }
        if (decommissionAttribute.attributeName() == null || Strings.isEmpty(decommissionAttribute.attributeName())) {
            validationException = addValidationError("attribute name is missing", validationException);
        }
        if (decommissionAttribute.attributeValue() == null || Strings.isEmpty(decommissionAttribute.attributeValue())) {
            validationException = addValidationError("attribute value is missing", validationException);
        }
        // This validation should not fail since we are not allowing delay timeout to be set externally.
        // Still keeping it for double check.
        if (noDelay && delayTimeout.getSeconds() > 0) {
            final String validationMessage = "Invalid decommission request. no_delay is true and delay_timeout is set to "
                + delayTimeout.getSeconds()
                + "] Seconds";
            validationException = addValidationError(validationMessage, validationException);
        }
        return validationException;
    }

    @Override
    public String toString() {
        return "DecommissionRequest{"
            + "decommissionAttribute="
            + decommissionAttribute
            + ", delayTimeout="
            + delayTimeout
            + ", noDelay="
            + noDelay
            + '}';
    }
}
