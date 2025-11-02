/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.distributed;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;

/**
 * Provides fallback strategies for primary term routing failures.
 * This class handles graceful degradation scenarios when primary term
 * routing encounters errors, ensuring the system continues to function
 * by falling back to the base directory.
 *
 * @opensearch.internal
 */
public class FallbackStrategy {

    private static final Logger logger = LogManager.getLogger(FallbackStrategy.class);

    /**
     * Handles primary term routing failures by falling back to the base directory.
     * This method is called when primary term access fails or routing encounters errors.
     *
     * @param filename the filename that failed to route
     * @param manager the directory manager to get the base directory from
     * @param cause the exception that caused the failure
     * @return the base directory as a fallback
     */
    public static Directory handlePrimaryTermFailure(String filename, PrimaryTermDirectoryManager manager, Exception cause) {
        logger.warn("Primary term routing failed for file '{}', falling back to base directory. Cause: {}", 
                   filename, cause.getMessage());
        
        if (logger.isDebugEnabled()) {
            logger.debug("Primary term routing failure details for file: " + filename, cause);
        }
        
        return manager.getBaseDirectory();
    }

    /**
     * Handles directory creation failures by falling back to the base directory.
     * This method is called when creating a new primary term directory fails.
     *
     * @param primaryTerm the primary term for which directory creation failed
     * @param manager the directory manager to get the base directory from
     * @param cause the exception that caused the failure
     * @return the base directory as a fallback
     */
    public static Directory handleDirectoryCreationFailure(long primaryTerm, PrimaryTermDirectoryManager manager, Exception cause) {
        logger.error("Failed to create directory for primary term {}, using base directory. Cause: {}", 
                    primaryTerm, cause.getMessage());
        
        if (logger.isDebugEnabled()) {
            logger.debug("Directory creation failure details for primary term: " + primaryTerm, cause);
        }
        
        return manager.getBaseDirectory();
    }

    /**
     * Handles directory validation failures by falling back to the base directory.
     * This method is called when directory accessibility or permission checks fail.
     *
     * @param primaryTerm the primary term for which validation failed
     * @param directoryPath the path that failed validation
     * @param manager the directory manager to get the base directory from
     * @param cause the exception that caused the failure
     * @return the base directory as a fallback
     */
    public static Directory handleDirectoryValidationFailure(long primaryTerm, String directoryPath, 
                                                           PrimaryTermDirectoryManager manager, Exception cause) {
        logger.warn("Directory validation failed for primary term {} at path '{}', using base directory. Cause: {}", 
                   primaryTerm, directoryPath, cause.getMessage());
        
        if (logger.isDebugEnabled()) {
            logger.debug("Directory validation failure details for primary term " + primaryTerm + " at path: " + directoryPath, cause);
        }
        
        return manager.getBaseDirectory();
    }

    /**
     * Handles IndexShard unavailability by using the default primary term.
     * This method is called when IndexShard context is not available for primary term access.
     *
     * @param operation the operation that was being attempted
     * @return the default primary term to use as fallback
     */
    public static long handleIndexShardUnavailable(String operation) {
        logger.debug("IndexShard unavailable for operation '{}', using default primary term", operation);
        return IndexShardContext.DEFAULT_PRIMARY_TERM;
    }

    /**
     * Creates a PrimaryTermRoutingException with appropriate error context.
     * This is a utility method for consistent exception creation with fallback logging.
     *
     * @param message the error message
     * @param errorType the type of error
     * @param primaryTerm the primary term associated with the error
     * @param filename the filename associated with the error, may be null
     * @param cause the underlying cause
     * @return a new PrimaryTermRoutingException
     */
    public static PrimaryTermRoutingException createRoutingException(String message, 
                                                                   PrimaryTermRoutingException.ErrorType errorType,
                                                                   long primaryTerm, 
                                                                   String filename, 
                                                                   Throwable cause) {
        PrimaryTermRoutingException exception = new PrimaryTermRoutingException(message, errorType, primaryTerm, filename, cause);
        
        logger.warn("Created routing exception: {}", exception.getMessage());
        if (logger.isDebugEnabled()) {
            logger.debug("Routing exception details", exception);
        }
        
        return exception;
    }

    /**
     * Logs a successful recovery from a fallback scenario.
     * This method should be called when the system successfully recovers from a failure.
     *
     * @param operation the operation that recovered
     * @param primaryTerm the primary term involved
     * @param filename the filename involved, may be null
     */
    public static void logSuccessfulRecovery(String operation, long primaryTerm, String filename) {
        if (filename != null) {
            logger.info("Successfully recovered from fallback for operation '{}' with primary term {} and file '{}'", 
                       operation, primaryTerm, filename);
        } else {
            logger.info("Successfully recovered from fallback for operation '{}' with primary term {}", 
                       operation, primaryTerm);
        }
    }
}