/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.decommission.awareness.delete;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.cluster.decommission.DecommissionAttribute;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.unit.TimeValue;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

public class DeleteDecommissionRequest extends ClusterManagerNodeRequest<DeleteDecommissionRequest> {
    private DecommissionAttribute decommissionAttribute;
    private TimeValue timeout;

    public DeleteDecommissionRequest() {}

    public DeleteDecommissionRequest(DecommissionAttribute decommissionAttribute, TimeValue timeOut) {
        this.decommissionAttribute = decommissionAttribute;
        this.timeout = timeOut;
    }

    public DeleteDecommissionRequest(StreamInput in) throws IOException {
        super(in);
        decommissionAttribute = new DecommissionAttribute(in);
        timeout = in.readTimeValue();
    }

    /**
     * Sets the decommission attribute name for decommission request
     *
     * @param timeout of the decommission attribute
     * @return the current object
     */
    public DeleteDecommissionRequest setName(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    /**
     * @return Returns the timeout value.
     */
    public TimeValue getTimeout() {
        return this.timeout;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (decommissionAttribute.attributeName() == null || decommissionAttribute.attributeName().isEmpty()) {
            validationException = addValidationError("attribute name is missing", validationException);
        }
        if (decommissionAttribute.attributeValue() == null || decommissionAttribute.attributeValue().isEmpty()) {
            validationException = addValidationError("attribute value is missing", validationException);
        }
        return validationException;
    }

    /**
     * @param timeout Sets the timeout value.
     */
    public void setTimeout(TimeValue timeout) {
        this.timeout = timeout;
    }

    /**
     * Sets decommission attribute for decommission request
     *
     * @param decommissionAttribute values that needs to be decommissioned
     * @return the current object
     */
    public DeleteDecommissionRequest setDecommissionAttribute(DecommissionAttribute decommissionAttribute) {
        this.decommissionAttribute = decommissionAttribute;
        return this;
    }

    /**
     * @return Returns the decommission attribute values
     */
    public DecommissionAttribute getDecommissionAttribute() {
        return this.decommissionAttribute;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        decommissionAttribute.writeTo(out);
        out.writeTimeValue(timeout);
    }

    @Override
    public String toString() {
        return "DeleteDecommissionRequest{" + "timeOut='" + this.timeout + '\'' + ", decommissionAttribute=" + decommissionAttribute + '}';
    }
}
