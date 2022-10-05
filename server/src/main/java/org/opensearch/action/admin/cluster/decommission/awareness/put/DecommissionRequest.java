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
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.unit.TimeValue;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Register decommission request.
 * <p>
 * Registers a decommission request with decommission attribute and timeout
 *
 * @opensearch.internal
 */
public class DecommissionRequest extends ClusterManagerNodeRequest<DecommissionRequest> {

    private DecommissionAttribute decommissionAttribute;
    private boolean retryOnClusterManagerChange;
    private TimeValue retryTimeout;

    public DecommissionRequest() {}

    public DecommissionRequest(DecommissionAttribute decommissionAttribute, boolean retryOnClusterManagerChange, TimeValue retryTimeout) {
        this.decommissionAttribute = decommissionAttribute;
        this.retryOnClusterManagerChange = retryOnClusterManagerChange;
        this.retryTimeout = retryTimeout;
    }

    public DecommissionRequest(DecommissionAttribute decommissionAttribute, TimeValue retryTimeout) {
        this(decommissionAttribute, false, retryTimeout);
    }

    public DecommissionRequest(StreamInput in) throws IOException {
        super(in);
        decommissionAttribute = new DecommissionAttribute(in);
        retryOnClusterManagerChange = in.readBoolean();
        retryTimeout = in.readTimeValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        decommissionAttribute.writeTo(out);
        out.writeBoolean(retryOnClusterManagerChange);
        out.writeTimeValue(retryTimeout);
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
     * Sets retryOnClusterManagerChange for decommission request
     *
     * @param retryOnClusterManagerChange boolean for request to retry decommission action on cluster manager change
     * @return this request
     */
    public DecommissionRequest setRetryOnClusterManagerChange(boolean retryOnClusterManagerChange) {
        this.retryOnClusterManagerChange = retryOnClusterManagerChange;
        return this;
    }

    /**
     * Sets the retry timeout for the request
     *
     * @param retryTimeout retry time out for the request
     * @return this request
     */
    public DecommissionRequest setRetryTimeout(TimeValue retryTimeout) {
        this.retryTimeout = retryTimeout;
        return this;
    }

    /**
     * @return Returns the decommission attribute key-value
     */
    public DecommissionAttribute getDecommissionAttribute() {
        return this.decommissionAttribute;
    }

    /**
     * @return Returns whether decommission is retry eligible on cluster manager change
     */
    public boolean retryOnClusterManagerChange() {
        return this.retryOnClusterManagerChange;
    }

    /**
     * @return retry timeout
     */
    public TimeValue getRetryTimeout() {
        return this.retryTimeout;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (decommissionAttribute.attributeName() == null || Strings.isEmpty(decommissionAttribute.attributeName())) {
            validationException = addValidationError("attribute name is missing", validationException);
        }
        if (decommissionAttribute.attributeValue() == null || Strings.isEmpty(decommissionAttribute.attributeValue())) {
            validationException = addValidationError("attribute value is missing", validationException);
        }
        return validationException;
    }

    @Override
    public String toString() {
        return "DecommissionRequest{"
            + "decommissionAttribute="
            + decommissionAttribute
            + ", retryOnClusterManagerChange="
            + retryOnClusterManagerChange
            + ", retryTimeout="
            + retryTimeout
            + '}';
    }
}
