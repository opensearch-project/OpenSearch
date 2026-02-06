/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.knn.engine;

import java.util.Collections;
import java.util.List;

import org.opensearch.common.ValidationException;

/**
 * KNNLibrary is an interface that helps the plugin communicate with k-NN libraries
 */
public interface KNNLibrary extends MethodResolver {

    /**
     * Gets the version of the library that is being used. In general, this can be used for ensuring compatibility of
     * serialized artifacts. For instance, this can be used to check if a given file that was created on a different
     * cluster is compatible with this instance of the library.
     *
     * @return the string representing the library's version
     */
    String getVersion();

    /**
     * Gets the extension that files written with this library should have
     *
     * @return extension
     */
    String getExtension();

    /**
     * Gets the compound extension that files written with this library should have
     *
     * @return compound extension
     */
    String getCompoundExtension();

    /**
     * Generate the Lucene score from the rawScore returned by the library. With k-NN, often times the library
     * will return a score where the lower the score, the better the result. This is the opposite of how Lucene scores
     * documents.
     *
     * @param rawScore  returned by the library
     * @param spaceType spaceType used to compute the score
     * @return Lucene score for the rawScore
     */
    float score(float rawScore, SpaceType spaceType);

    /**
     * Translate the distance radius input from end user to the engine's threshold.
     *
     * @param distance distance radius input from end user
     * @param spaceType spaceType used to compute the radius
     *
     * @return transformed distance for the library
     */
    Float distanceToRadialThreshold(Float distance, SpaceType spaceType);

    /**
     * Translate the score threshold input from end user to the engine's threshold.
     *
     * @param score score threshold input from end user
     * @param spaceType spaceType used to compute the threshold
     *
     * @return transformed score for the library
     */
    Float scoreToRadialThreshold(Float score, SpaceType spaceType);

    /**
     * Validate the knnMethodContext for the given library. A ValidationException should be thrown if the method is
     * deemed invalid.
     *
     * @param knnMethodContext to be validated
     * @param knnMethodConfigContext configuration context for the method
     * @return ValidationException produced by validation errors; null if no validations errors.
     */
    ValidationException validateMethod(KNNMethodContext knnMethodContext, KNNMethodConfigContext knnMethodConfigContext);

    /**
     * Returns whether training is required or not from knnMethodContext for the given library.
     *
     * @param knnMethodContext methodContext
     * @return true if training is required; false otherwise
     */
    boolean isTrainingRequired(KNNMethodContext knnMethodContext);

    /**
     * Estimate overhead of KNNMethodContext in Kilobytes.
     *
     * @param knnMethodContext to estimate size for
     * @param knnMethodConfigContext configuration context for the method
     * @return size overhead estimate in KB
     */
    int estimateOverheadInKB(KNNMethodContext knnMethodContext, KNNMethodConfigContext knnMethodConfigContext);

    /**
     * Get the context from the library needed to build the index.
     *
     * @param knnMethodContext to get build context for
     * @param knnMethodConfigContext configuration context for the method
     * @return parameter map
     */
    KNNLibraryIndexingContext getKNNLibraryIndexingContext(
        KNNMethodContext knnMethodContext,
        KNNMethodConfigContext knnMethodConfigContext
    );

    /**
     * Gets metadata related to methods supported by the library
     *
     * @param methodName name of method
     * @return KNNLibrarySearchContext
     */
    KNNLibrarySearchContext getKNNLibrarySearchContext(String methodName);

    /**
     * Getter for initialized
     *
     * @return whether library has been initialized
     */
    Boolean isInitialized();

    /**
     * Set initialized to true
     *
     * @param isInitialized whether library has been initialized
     */
    void setInitialized(Boolean isInitialized);

    /**
     * Getter for mmap file extensions
     *
     * @return list of file extensions that will be read/write with mmap
     */
    default List<String> mmapFileExtensions() {
        return Collections.emptyList();
    }
}
