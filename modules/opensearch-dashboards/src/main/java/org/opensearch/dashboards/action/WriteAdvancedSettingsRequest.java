/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dashboards.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Map;

import static org.opensearch.action.search.SearchRequest.DEFAULT_INDICES_OPTIONS;

public class WriteAdvancedSettingsRequest extends ActionRequest implements IndicesRequest.Replaceable {

    public enum OperationType {
        CREATE,
        UPDATE
    }

    private String index;
    private Map<String, Object> settings;
    private OperationType operationType;

    public WriteAdvancedSettingsRequest() {
        this.operationType = OperationType.UPDATE;
    }

    public WriteAdvancedSettingsRequest(String index, Map<String, Object> settings) {
        this.index = index;
        this.settings = settings;
        this.operationType = OperationType.UPDATE;
    }

    public WriteAdvancedSettingsRequest(String index, Map<String, Object> settings, OperationType operationType) {
        this.index = index;
        this.settings = settings;
        this.operationType = operationType;
    }

    public WriteAdvancedSettingsRequest(StreamInput in) throws IOException {
        super(in);
        this.index = in.readString();
        this.settings = in.readMap();
        this.operationType = in.readEnum(OperationType.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeMap(settings);
        out.writeEnum(operationType);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public String getIndex() {
        return index;
    }

    public Map<String, Object> getSettings() {
        return settings;
    }

    public OperationType getOperationType() {
        return operationType;
    }

    public boolean isCreateOperation() {
        return operationType == OperationType.CREATE;
    }

    @Override
    public IndicesRequest indices(String... indices) {
        index = indices[0];
        return this;
    }

    @Override
    public String[] indices() {
        return new String[] { this.index };
    }

    @Override
    public IndicesOptions indicesOptions() {
        return DEFAULT_INDICES_OPTIONS;
    }
}
