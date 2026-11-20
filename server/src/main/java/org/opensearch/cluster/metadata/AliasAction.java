/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.common.Nullable;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.action.admin.indices.AliasesNotFoundException;

/**
 * Individual operation to perform on the cluster state as part of an {@link IndicesAliasesRequest}.
 *
 * @opensearch.internal
 */
public abstract class AliasAction {
    private final String index;

    private AliasAction(String index) {
        if (false == Strings.hasText(index)) {
            throw new IllegalArgumentException("[index] is required");
        }
        this.index = index;
    }

    /**
     * Get the index on which the operation should act.
     */
    public String getIndex() {
        return index;
    }

    /**
     * Should this action remove the index? Actions that return true from this will never execute
     * {@link #apply(NewAliasValidator, Metadata.Builder, IndexMetadata)}.
     */
    abstract boolean removeIndex();

    /**
     * Apply the action.
     *
     * @param aliasValidator call to validate a new alias before adding it to the builder
     * @param metadata metadata builder for the changes made by all actions as part of this request
     * @param index metadata for the index being changed
     * @return did this action make any changes?
     */
    abstract boolean apply(NewAliasValidator aliasValidator, Metadata.Builder metadata, IndexMetadata index);

    /**
     * Validate a new alias.
     *
     * @opensearch.internal
     */
    @FunctionalInterface
    public interface NewAliasValidator {
        void validate(String alias, @Nullable String indexRouting, @Nullable String filter, @Nullable Boolean writeIndex);
    }

    /**
     * Operation to add an alias to an index.
     *
     * @opensearch.internal
     */
    public static class Add extends AliasAction {
        private final String alias;

        @Nullable
        private final String filter;

        @Nullable
        private final String indexRouting;

        @Nullable
        private final String searchRouting;

        @Nullable
        private final Boolean writeIndex;

        @Nullable
        final Boolean isHidden;

        @Nullable
        private final String scoringMode;

        private Add(Builder builder) {
            super(builder.index);
            if (false == Strings.hasText(builder.alias)) {
                throw new IllegalArgumentException("[alias] is required");
            }
            if (builder.scoringMode != null && Strings.hasText(builder.filter) == false) {
                throw new IllegalArgumentException("[scoring_mode] requires a [filter] on alias [" + builder.alias + "]");
            }
            this.alias = builder.alias;
            this.filter = builder.filter;
            this.indexRouting = builder.indexRouting;
            this.searchRouting = builder.searchRouting;
            this.writeIndex = builder.writeIndex;
            this.isHidden = builder.isHidden;
            this.scoringMode = builder.scoringMode;
        }

        /**
         * Starts an operation that adds {@code alias} to {@code index}; every other property is optional.
         *
         * @param index the index to add the alias to
         * @param alias the alias name
         * @return a builder for the operation
         */
        public static Builder builder(String index, String alias) {
            return new Builder(index, alias);
        }

        /**
         * Builds an {@link Add} operation.
         *
         * @opensearch.internal
         */
        public static final class Builder {
            private final String index;
            private final String alias;
            private String filter;
            private String indexRouting;
            private String searchRouting;
            private Boolean writeIndex;
            private Boolean isHidden;
            private String scoringMode;

            private Builder(String index, String alias) {
                this.index = index;
                this.alias = alias;
            }

            /** Sets the alias filter as a JSON query. */
            public Builder filter(@Nullable String filter) {
                this.filter = filter;
                return this;
            }

            /** Sets the routing used when indexing through the alias. */
            public Builder indexRouting(@Nullable String indexRouting) {
                this.indexRouting = indexRouting;
                return this;
            }

            /** Sets the routing used when searching through the alias. */
            public Builder searchRouting(@Nullable String searchRouting) {
                this.searchRouting = searchRouting;
                return this;
            }

            /** Sets whether the index is the write index of the alias. */
            public Builder writeIndex(@Nullable Boolean writeIndex) {
                this.writeIndex = writeIndex;
                return this;
            }

            /** Sets whether the alias is hidden. */
            public Builder isHidden(@Nullable Boolean isHidden) {
                this.isHidden = isHidden;
                return this;
            }

            /** Sets the scoring mode; it requires a filter. */
            public Builder scoringMode(@Nullable String scoringMode) {
                this.scoringMode = scoringMode;
                return this;
            }

            /**
             * Builds the operation.
             *
             * @return the operation
             * @throws IllegalArgumentException if the alias is empty, or a scoring mode is set without a filter
             */
            public Add build() {
                return new Add(this);
            }
        }

        /**
         * Alias to add to the index.
         */
        public String getAlias() {
            return alias;
        }

        public String getFilter() {
            return filter;
        }

        public String getSearchRouting() {
            return searchRouting;
        }

        public String getIndexRouting() {
            return indexRouting;
        }

        public Boolean writeIndex() {
            return writeIndex;
        }

        @Nullable
        public Boolean isHidden() {
            return isHidden;
        }

        @Nullable
        public String getScoringMode() {
            return scoringMode;
        }

        @Override
        boolean removeIndex() {
            return false;
        }

        @Override
        boolean apply(NewAliasValidator aliasValidator, Metadata.Builder metadata, IndexMetadata index) {
            aliasValidator.validate(alias, indexRouting, filter, writeIndex);

            AliasMetadata newAliasMd = AliasMetadata.newAliasMetadataBuilder(alias)
                .filter(filter)
                .indexRouting(indexRouting)
                .searchRouting(searchRouting)
                .writeIndex(writeIndex)
                .isHidden(isHidden)
                .scoringMode(scoringMode)
                .build();

            // Check if this alias already exists
            AliasMetadata currentAliasMd = index.getAliases().get(alias);
            if (currentAliasMd != null && currentAliasMd.equals(newAliasMd)) {
                // It already exists, ignore it
                return false;
            }

            metadata.put(IndexMetadata.builder(index).putAlias(newAliasMd));
            return true;
        }
    }

    /**
     * Operation to remove an alias from an index.
     *
     * @opensearch.internal
     */
    public static class Remove extends AliasAction {
        private final String alias;
        @Nullable
        private final Boolean mustExist;

        /**
         * Build the operation.
         */
        public Remove(String index, String alias, @Nullable Boolean mustExist) {
            super(index);
            if (false == Strings.hasText(alias)) {
                throw new IllegalArgumentException("[alias] is required");
            }
            this.alias = alias;
            this.mustExist = mustExist;
        }

        /**
         * Alias to remove from the index.
         */
        public String getAlias() {
            return alias;
        }

        @Override
        boolean removeIndex() {
            return false;
        }

        @Override
        boolean apply(NewAliasValidator aliasValidator, Metadata.Builder metadata, IndexMetadata index) {
            if (false == index.getAliases().containsKey(alias)) {
                if (mustExist != null && mustExist) {
                    throw new AliasesNotFoundException(alias);
                }
                return false;
            }
            metadata.put(IndexMetadata.builder(index).removeAlias(alias));
            return true;
        }
    }

    /**
     * Operation to remove an index. This is an "alias action" because it allows us to remove an index at the same time as we remove add an
     * alias to replace it.
     *
     * @opensearch.internal
     */
    public static class RemoveIndex extends AliasAction {
        public RemoveIndex(String index) {
            super(index);
        }

        @Override
        boolean removeIndex() {
            return true;
        }

        @Override
        boolean apply(NewAliasValidator aliasValidator, Metadata.Builder metadata, IndexMetadata index) {
            throw new UnsupportedOperationException();
        }
    }
}
