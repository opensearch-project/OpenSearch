
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore;

import java.time.Instant;

/**
 * Utility classes supporting conditional write operations on a {@link BlobContainer}.
 * The main entry points are {@link ConditionalWriteOptions} for specifying conditions, and
 * {@link ConditionalWriteResponse} for receiving the result of a conditional write.
 */
public final class ConditionalWrite {
    private ConditionalWrite() {}

    /**
     * Encapsulates options controlling preconditions to be deployed when a blob is to be written to the remote store.
     * Immutable and thread-safe. Use the provided static factory methods or the {@link Builder}
     * to construct instances with the desired conditions. These options can be supplied to
     * blob store write operations to enforce preconditions
     *
     */
    public static final class ConditionalWriteOptions {

        private final boolean ifNotExists;
        private final boolean ifMatch;
        private final boolean ifUnmodifiedSince;
        private final String versionIdentifier;
        private final Instant unmodifiedSince;

        private ConditionalWriteOptions(Builder builder) {
            this.ifNotExists = builder.ifNotExists;
            this.ifMatch = builder.ifMatch;
            this.ifUnmodifiedSince = builder.ifUnmodifiedSince;
            this.versionIdentifier = builder.versionIdentifier;
            this.unmodifiedSince = builder.unmodifiedSince;
        }

        public static ConditionalWriteOptions none() {
            return new Builder().build();
        }

        public static ConditionalWriteOptions ifNotExists() {
            return new Builder().setIfNotExists(true).build();
        }

        public static ConditionalWriteOptions ifMatch(String versionIdentifier) {
            return new Builder().setIfMatch(true).setVersionIdentifier(versionIdentifier).build();
        }

        public static ConditionalWriteOptions ifUnmodifiedSince(Instant ts) {
            return new Builder().setIfUnmodifiedSince(true).setUnmodifiedSince(ts).build();
        }

        /**
         * Returns a new {@link Builder} for constructing custom conditional write options.
         */
        public static Builder builder() {
            return new Builder();
        }

        public boolean isIfNotExists() {
            return ifNotExists;
        }

        public boolean isIfMatch() {
            return ifMatch;
        }

        public boolean isIfUnmodifiedSince() {
            return ifUnmodifiedSince;
        }

        public String getVersionIdentifier() {
            return versionIdentifier;
        }

        public Instant getUnmodifiedSince() {
            return unmodifiedSince;
        }

        public static final class Builder {
            private boolean ifNotExists = false;
            private boolean ifMatch = false;
            private boolean ifUnmodifiedSince = false;
            private String versionIdentifier = null;
            private Instant unmodifiedSince = null;

            private Builder() {}

            /**
             * Sets the write to succeed only if the blob does not exist.
             * @param flag true to enable this condition
             * @return this builder
             */
            public Builder setIfNotExists(boolean flag) {
                this.ifNotExists = flag;
                return this;
            }

            /**
             * Sets the write to succeed only if the blob matches the expected version.
             * @param flag true to enable this condition
             * @return this builder
             */
            public Builder setIfMatch(boolean flag) {
                this.ifMatch = flag;
                return this;
            }

            /**
             * Sets the write to succeed only if the blob was not modified since a given instant.
             * @param flag true to enable this condition
             * @return this builder
             */
            public Builder setIfUnmodifiedSince(boolean flag) {
                this.ifUnmodifiedSince = flag;
                return this;
            }

            /**
             * Sets the timestamp before which the blob must remain unmodified.
             * @param ts the instant to check
             * @return this builder
             */
            public Builder setUnmodifiedSince(Instant ts) {
                this.unmodifiedSince = ts;
                return this;
            }

            public Builder setVersionIdentifier(String versionIdentifier) {
                this.versionIdentifier = versionIdentifier;
                return this;
            }

            public ConditionalWriteOptions build() {
                return new ConditionalWriteOptions(this);
            }
        }
    }

    /**
     * encapsulates the result of a conditional write operation.
     * Contains the new version identifier (such as an ETag or version string) retrieved from the remote store
     * after a successful write.
     */
    public static final class ConditionalWriteResponse {
        private final String newVersionIdentifier;

        private ConditionalWriteResponse(String versionIdentifier) {
            this.newVersionIdentifier = versionIdentifier;
        }

        public static ConditionalWriteResponse success(String versionIdentifier) {
            return new ConditionalWriteResponse(versionIdentifier);
        }

        public String getVersionIdentifier() {
            return newVersionIdentifier;
        }
    }
}
