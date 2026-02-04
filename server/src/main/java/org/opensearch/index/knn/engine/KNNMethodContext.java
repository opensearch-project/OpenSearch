package org.opensearch.index.knn.engine;

import java.io.IOException;

import org.opensearch.common.ValidationException;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

/**
 * KNNMethodContext will contain the information necessary to produce a library index from an Opensearch mapping.
 * It will encompass all parameters necessary to build the index.
 */
public interface KNNMethodContext extends ToXContentFragment, Writeable {
    /**
     * This method uses the knnEngine to validate that the method is compatible with the engine.
     *
     * @param knnMethodConfigContext context to validate against
     * @return ValidationException produced by validation errors; null if no validations errors.
     */
    public ValidationException validate(KNNMethodConfigContext knnMethodConfigContext);

    /**
     * This method returns whether training is requires or not from knnEngine
     *
     * @return true if training is required by knnEngine; false otherwise
     */
    public boolean isTrainingRequired();

    /**
     * This method estimates the overhead the knn method adds irrespective of the number of vectors
     *
     * @param knnMethodConfigContext context to estimate overhead
     * @return size in Kilobytes
     */
    public int estimateOverheadInKB(KNNMethodConfigContext knnMethodConfigContext);

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException;

    @Override
    public boolean equals(Object obj);

    @Override
    public int hashCode();

    @Override
    public void writeTo(StreamOutput out) throws IOException;
}
