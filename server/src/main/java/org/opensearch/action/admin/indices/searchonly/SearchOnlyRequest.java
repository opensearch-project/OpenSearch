package org.opensearch.action.admin.indices.searchonly;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ValidateActions;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.clustermanager.AcknowledgedRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public class SearchOnlyRequest extends AcknowledgedRequest<SearchOnlyRequest> {
    private String[] indices;
    private boolean scaleDown;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();

    public SearchOnlyRequest(String index) {
        this(new String[] { Objects.requireNonNull(index) }, false);
    }

    public SearchOnlyRequest(String[] indices, boolean scaleDown) {
        this.indices = Objects.requireNonNull(indices);
        this.scaleDown = scaleDown;
    }

    public SearchOnlyRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        scaleDown = in.readBoolean();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        out.writeBoolean(scaleDown);
        indicesOptions.writeIndicesOptions(out);
    }

    public String[] indices() {
        return indices;
    }

    public boolean isScaleDown() {
        return scaleDown;
    }

    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public SearchOnlyRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (indices == null || indices.length == 0) {
            validationException = ValidateActions.addValidationError("index/indices is missing", validationException);
        } else {
            for (String index : indices) {
                if (index == null || index.trim().isEmpty()) {
                    validationException = ValidateActions.addValidationError(
                        "index/indices contains null or empty value",
                        validationException
                    );
                    break;
                }
            }
        }
        return validationException;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchOnlyRequest that = (SearchOnlyRequest) o;
        return scaleDown == that.scaleDown && Arrays.equals(indices, that.indices) && Objects.equals(indicesOptions, that.indicesOptions);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(scaleDown, indicesOptions);
        result = 31 * result + Arrays.hashCode(indices);
        return result;
    }

    /**
     * Sets whether this is a scale down operation
     * @param scaleDown true if scaling down, false if scaling up
     * @return this request
     */
    public SearchOnlyRequest scaleDown(boolean scaleDown) {
        this.scaleDown = scaleDown;
        return this;
    }
}
