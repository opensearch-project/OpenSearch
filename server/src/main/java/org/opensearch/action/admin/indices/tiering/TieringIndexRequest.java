/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.tiering;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.master.AcknowledgedRequest;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Represents the tiering request for indices to move to a different tier
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class TieringIndexRequest extends AcknowledgedRequest<TieringIndexRequest> implements IndicesRequest.Replaceable {

    private String[] indices;
    private final Tier targetTier;
    private IndicesOptions indicesOptions;
    private boolean waitForCompletion;

    public TieringIndexRequest(String targetTier, String... indices) {
        this.targetTier = Tier.fromString(targetTier);
        this.indices = indices;
        this.indicesOptions = IndicesOptions.fromOptions(false, false, true, false);
        this.waitForCompletion = false;
    }

    public TieringIndexRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        targetTier = Tier.fromString(in.readString());
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        waitForCompletion = in.readBoolean();
    }

    // pkg private for testing
    TieringIndexRequest(Tier targetTier, IndicesOptions indicesOptions, boolean waitForCompletion, String... indices) {
        this.indices = indices;
        this.targetTier = targetTier;
        this.indicesOptions = indicesOptions;
        this.waitForCompletion = waitForCompletion;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (indices == null) {
            validationException = addValidationError("Mandatory parameter - indices is missing from the request", validationException);
        } else {
            for (String index : indices) {
                if (index == null || index.length() == 0) {
                    validationException = addValidationError(
                        String.format(Locale.ROOT, "Specified index in the request [%s] is null or empty", index),
                        validationException
                    );
                }
            }
        }
        if (!Tier.WARM.equals(targetTier)) {
            validationException = addValidationError("The specified tier is not supported", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        out.writeString(targetTier.value());
        indicesOptions.writeIndicesOptions(out);
        out.writeBoolean(waitForCompletion);
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    @Override
    public TieringIndexRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    public TieringIndexRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    /**
     * If this parameter is set to true the operation will wait for completion of tiering process before returning.
     *
     * @param waitForCompletion if true the operation will wait for completion
     * @return this request
     */
    public TieringIndexRequest waitForCompletion(boolean waitForCompletion) {
        this.waitForCompletion = waitForCompletion;
        return this;
    }

    /**
     * Returns wait for completion setting
     *
     * @return true if the operation will wait for completion
     */
    public boolean waitForCompletion() {
        return waitForCompletion;
    }

    public Tier tier() {
        return targetTier;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TieringIndexRequest that = (TieringIndexRequest) o;
        return clusterManagerNodeTimeout.equals(that.clusterManagerNodeTimeout)
            && timeout.equals(that.timeout)
            && Objects.equals(indicesOptions, that.indicesOptions)
            && Arrays.equals(indices, that.indices)
            && targetTier.equals(that.targetTier)
            && waitForCompletion == that.waitForCompletion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterManagerNodeTimeout, timeout, indicesOptions, waitForCompletion, Arrays.hashCode(indices));
    }

    /**
     * Represents the supported tiers for an index
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    public enum Tier {
        HOT,
        WARM;

        public static Tier fromString(String name) {
            if (name == null) {
                throw new IllegalArgumentException("Tiering type cannot be null");
            }
            String upperCase = name.trim().toUpperCase(Locale.ROOT);
            switch (upperCase) {
                case "HOT":
                    return HOT;
                case "WARM":
                    return WARM;
                default:
                    throw new IllegalArgumentException(
                        "Tiering type [" + name + "] is not supported. Supported types are " + HOT + " and " + WARM
                    );
            }
        }

        public String value() {
            return name().toLowerCase(Locale.ROOT);
        }
    }
}
