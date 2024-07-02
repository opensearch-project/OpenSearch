/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.tiering;

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
 * Represents the tiering request for indices
 * to move to a different tier
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class TieringIndexRequest extends AcknowledgedRequest<TieringIndexRequest> implements IndicesRequest.Replaceable {

    private String[] indices;
    private Tier targetTier;
    private IndicesOptions indicesOptions = IndicesOptions.fromOptions(false, false, true, false);
    private boolean waitForCompletion;

    public TieringIndexRequest() {}

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (indices == null || indices.length == 0) {
            validationException = addValidationError("Mandatory parameter - indices is missing from the request", validationException);
        }
        if (targetTier == null) {
            validationException = addValidationError("Mandatory parameter - tier is missing from the request", validationException);
        }
        if (Tier.HOT.equals(targetTier)) {
            validationException = addValidationError("The specified tiering to hot is not supported yet", validationException);
        }
        return validationException;
    }

    public TieringIndexRequest(String targetTier, String... indices) {
        this.targetTier = Tier.fromString(targetTier);
        this.indices = indices;
    }

    public TieringIndexRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        targetTier = Tier.fromString(in.readString());
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        waitForCompletion = in.readBoolean();
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

    public TieringIndexRequest tier(Tier tier) {
        this.targetTier = tier;
        return this;
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
            String upperCase = name.trim().toUpperCase(Locale.ROOT);
            if (HOT.name().equals(upperCase)) {
                return HOT;
            }
            if (WARM.name().equals(upperCase)) {
                return WARM;
            }
            throw new IllegalArgumentException("Tiering type [" + name + "] is not supported. Supported types are " + HOT + " and " + WARM);
        }

        public String value() {
            return name().toLowerCase(Locale.ROOT);
        }
    }
}
