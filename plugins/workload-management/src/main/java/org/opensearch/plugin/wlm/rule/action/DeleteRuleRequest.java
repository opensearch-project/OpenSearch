/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.action;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A request for deleting a Rule
 * @opensearch.experimental
 */
public class DeleteRuleRequest extends ClusterManagerNodeRequest<DeleteRuleRequest> {
    private final String _id;

    /**
     * Constructor for DeleteRuleRequest
     * @param _id - Rule _id that we want to delete
     */
    public DeleteRuleRequest(String _id) {
        this._id = _id;
    }

    /**
     * Constructor for DeleteRuleRequest
     * @param in - A {@link StreamInput} object
     */
    public DeleteRuleRequest(StreamInput in) throws IOException {
        super(in);
        _id = in.readOptionalString();
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(_id);
    }

    /**
     * _id getter
     */
    public String get_id() {
        return _id;
    }
}
