/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.wlm.MutableQueryGroupFragment;

import java.io.IOException;

/**
 * A request for update QueryGroup
 *
 * @opensearch.experimental
 */
public class UpdateQueryGroupRequest extends ActionRequest {
    private final String name;
    private final MutableQueryGroupFragment mutableQueryGroupFragment;

    /**
     * Constructor for UpdateQueryGroupRequest
     * @param name - QueryGroup name for UpdateQueryGroupRequest
     * @param mutableQueryGroupFragment - MutableQueryGroupFragment for UpdateQueryGroupRequest
     */
    UpdateQueryGroupRequest(String name, MutableQueryGroupFragment mutableQueryGroupFragment) {
        this.name = name;
        this.mutableQueryGroupFragment = mutableQueryGroupFragment;
    }

    /**
     * Constructor for UpdateQueryGroupRequest
     * @param in - A {@link StreamInput} object
     */
    UpdateQueryGroupRequest(StreamInput in) throws IOException {
        this(in.readString(), new MutableQueryGroupFragment(in));
    }

    /**
     * Generate a UpdateQueryGroupRequest from XContent
     * @param parser - A {@link XContentParser} object
     * @param name - name of the QueryGroup to be updated
     */
    public static UpdateQueryGroupRequest fromXContent(XContentParser parser, String name) throws IOException {
        QueryGroup.Builder builder = QueryGroup.Builder.fromXContent(parser);
        return new UpdateQueryGroupRequest(name, builder.getMutableQueryGroupFragment());
    }

    @Override
    public ActionRequestValidationException validate() {
        QueryGroup.validateName(name);
        return null;
    }

    /**
     * name getter
     */
    public String getName() {
        return name;
    }

    /**
     * mutableQueryGroupFragment getter
     */
    public MutableQueryGroupFragment getmMutableQueryGroupFragment() {
        return mutableQueryGroupFragment;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        mutableQueryGroupFragment.writeTo(out);
    }
}
