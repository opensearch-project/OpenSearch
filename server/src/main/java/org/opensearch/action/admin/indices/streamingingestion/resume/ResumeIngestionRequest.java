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
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * A request to resume ingestion.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ResumeIngestionRequest extends AcknowledgedRequest<ResumeIngestionRequest> implements IndicesRequest.Replaceable {
    public static final String RESET_SETTINGS = "reset_settings";
    private String[] indices;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();
    private ResetSettings[] resetSettings;

    public ResumeIngestionRequest(StreamInput in) throws IOException {
        super(in);
        this.indices = in.readStringArray();
        this.indicesOptions = IndicesOptions.readIndicesOptions(in);
        this.resetSettings = in.readArray(ResetSettings::new, ResetSettings[]::new);
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
    public ResumeIngestionRequest(String[] indices, ResetSettings[] resetSettings) {
        this.indices = indices;
        this.resetSettings = resetSettings;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (CollectionUtils.isEmpty(indices)) {
            validationException = addValidationError("index is missing", validationException);
        }

        if (resetSettings.length > 0) {
            boolean invalidResetSettingsFound = Arrays.stream(resetSettings)
                .anyMatch(
                    resetSettings -> resetSettings.getShard() < 0 || resetSettings.getMode() == null || resetSettings.getValue() == null
                );
            if (invalidResetSettingsFound) {
                validationException = addValidationError("ResetSettings is missing either shard, mode or value", validationException);
            }
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
        out.writeArray(resetSettings);
    }

    public ResetSettings[] getResetSettings() {
        return resetSettings;
    }

    public static ResumeIngestionRequest fromXContent(String[] indices, XContentParser parser) throws IOException {
        if (parser.currentToken() == null) {
            parser.nextToken();
        }

        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("Expected START_OBJECT but got: " + parser.currentToken());
        }

        ArrayList<ResetSettings> resetSettingsList = new ArrayList<>();

        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String currentFieldName = parser.currentName();
            parser.nextToken();

            if (RESET_SETTINGS.equals(currentFieldName)) {
                if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
                    throw new IllegalArgumentException("Expected START_ARRAY for 'reset_settings'");
                }

                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    resetSettingsList.add(ResetSettings.fromXContent(parser));
                }

            } else {
                throw new IllegalArgumentException("Unexpected field: " + currentFieldName);
            }
        }

        return new ResumeIngestionRequest(indices, resetSettingsList.toArray(new ResetSettings[0]));
    }

    /**
     * Represents reset settings for a given shard to be applied as part of resume operation.
     * @opensearch.experimental
     */
    @ExperimentalApi
    public static class ResetSettings implements Writeable {
        private final int shard;
        private final ResetMode mode;
        private final String value;

        public ResetSettings(int shard, ResetMode mode, String value) {
            this.shard = shard;
            this.mode = mode;
            this.value = value;
        }

        public ResetSettings(StreamInput in) throws IOException {
            this.shard = in.readVInt();
            this.mode = in.readEnum(ResetMode.class);
            this.value = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(shard);
            out.writeEnum(mode);
            out.writeString(value);
        }

        public int getShard() {
            return shard;
        }

        public ResetMode getMode() {
            return mode;
        }

        public String getValue() {
            return value;
        }

        /**
         * Reset options for Resume API. Offset mode supports kafka offsets or Kinesis sequence numbers and timestamp
         * mode supports a timestamp in milliseconds that will be used to retrieve corresponding offset.
         */
        @ExperimentalApi
        public enum ResetMode {
            OFFSET,
            TIMESTAMP
        }

        public static ResetSettings fromXContent(XContentParser parser) throws IOException {
            if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("Expected START_OBJECT for ResetSettings but got: " + parser.currentToken());
            }

            int shard = -1;
            ResetMode mode = null;
            String value = null;

            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                String fieldName = parser.currentName();
                parser.nextToken();

                switch (fieldName) {
                    case "shard" -> shard = parser.intValue();
                    case "mode" -> {
                        try {
                            mode = ResetMode.valueOf(parser.text().toUpperCase(Locale.ROOT));
                        } catch (IllegalArgumentException e) {
                            throw new IllegalArgumentException("Invalid value for 'mode': " + parser.text());
                        }
                    }
                    case "value" -> value = parser.text();
                    default -> throw new IllegalArgumentException("Unexpected field in ResetSettings: " + fieldName);
                }
            }

            if (shard < 0 || mode == null || value == null) {
                throw new IllegalArgumentException(
                    "Missing required fields in ResetSettings: shard=" + shard + ", mode=" + mode + ", value=" + value
                );
            }

            return new ResetSettings(shard, mode, value);
        }

    }
}
