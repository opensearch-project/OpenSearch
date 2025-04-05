/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.streamingingestion.resume;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.clustermanager.AcknowledgedRequest;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.util.CollectionUtils;

import java.io.IOException;
import java.util.Arrays;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * A request to resume ingestion.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ResumeIngestionRequest extends AcknowledgedRequest<ResumeIngestionRequest> implements IndicesRequest.Replaceable {

    private String[] indices;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();
    // todo: support reset settings
    private final ResetSettings[] resetSettingsList;

    public ResumeIngestionRequest(StreamInput in) throws IOException {
        super(in);
        this.indices = in.readStringArray();
        this.indicesOptions = IndicesOptions.readIndicesOptions(in);
        this.resetSettingsList = in.readArray(ResetSettings::new, ResetSettings[]::new);
    }

    /**
     * Constructs a new resume ingestion request.
     */
    public ResumeIngestionRequest(String[] indices) {
        this(indices, new ResetSettings[0]);
    }

    /**
     * Constructs a new resume ingestion request with reset settings.
     */
    public ResumeIngestionRequest(String[] indices, ResetSettings[] resetSettingsList) {
        this.indices = indices;
        this.resetSettingsList = resetSettingsList;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (CollectionUtils.isEmpty(indices)) {
            validationException = addValidationError("index is missing", validationException);
        }

        if (resetSettingsList.length > 0) {
            boolean invalidResetSettingsFound = Arrays.stream(resetSettingsList)
                .anyMatch(
                    resetSettings -> resetSettings.getShard() < 0 || resetSettings.getMode() == null || resetSettings.getValue() == null
                );
            if (invalidResetSettingsFound) {
                validationException = addValidationError("ResetSettings is missing either shard, mode or value", validationException);
            }

            // todo: remove this when reset settings support is added
            validationException = addValidationError("reset settings is currently not supported", validationException);
        }
        return validationException;
    }

    /**
     * The indices to be resumed
     */
    @Override
    public String[] indices() {
        return indices;
    }

    /**
     * Sets the indices to be resumed
     */
    @Override
    public ResumeIngestionRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and how to deal with wildcard expressions.
     * For example indices that don't exist.
     *
     * @return the desired behaviour regarding indices to ignore and wildcard indices expressions
     */
    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    /**
     * Specifies what type of requested indices to ignore and how to deal wild wildcard expressions.
     * For example indices that don't exist.
     *
     * @param indicesOptions the desired behaviour regarding indices to ignore and wildcard indices expressions
     * @return the request itself
     */
    public ResumeIngestionRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        out.writeArray(resetSettingsList);
    }

    /**
     * Represents reset settings for a given shard to be applied as part of resume operation.
     * @opensearch.experimental
     */
    @ExperimentalApi
    public static class ResetSettings implements Writeable {
        private final int shard;
        private final String mode;
        private final String value;

        public ResetSettings(int shard, String mode, String value) {
            this.shard = shard;
            this.mode = mode;
            this.value = value;
        }

        public ResetSettings(StreamInput in) throws IOException {
            this.shard = in.readVInt();
            this.mode = in.readString();
            this.value = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(shard);
            out.writeString(mode);
            out.writeString(value);
        }

        public int getShard() {
            return shard;
        }

        public String getMode() {
            return mode;
        }

        public String getValue() {
            return value;
        }
    }
}
