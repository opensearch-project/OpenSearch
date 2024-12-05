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

import org.opensearch.OpenSearchParseException;
import org.opensearch.Version;
import org.opensearch.cluster.AbstractNamedDiffable;
import org.opensearch.cluster.NamedDiff;
import org.opensearch.cluster.metadata.Metadata.Custom;
import org.opensearch.common.Nullable;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.repositories.RepositoryData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

import static org.opensearch.repositories.blobstore.BlobStoreRepository.SYSTEM_REPOSITORY_SETTING;

/**
 * Contains metadata about registered snapshot repositories
 *
 * @opensearch.internal
 */
public class RepositoriesMetadata extends AbstractNamedDiffable<Custom> implements Custom {

    public static final String TYPE = "repositories";

    /**
     * Serialization parameter used to hide the {@link RepositoryMetadata#generation()} and {@link RepositoryMetadata#pendingGeneration()}
     * in {@link org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesResponse}.
     */
    public static final String HIDE_GENERATIONS_PARAM = "hide_generations";
    public static final String HIDE_SYSTEM_REPOSITORY_SETTING = "hide_system_repository_setting";

    private final List<RepositoryMetadata> repositories;

    /**
     * Constructs new repository metadata
     *
     * @param repositories list of repositories
     */
    public RepositoriesMetadata(List<RepositoryMetadata> repositories) {
        this.repositories = Collections.unmodifiableList(repositories);
    }

    /**
     * Creates a new instance that has the given repository moved to the given {@code safeGeneration} and {@code pendingGeneration}.
     *
     * @param repoName          repository name
     * @param safeGeneration    new safe generation
     * @param pendingGeneration new pending generation
     * @return new instance with updated generations
     */
    public RepositoriesMetadata withUpdatedGeneration(String repoName, long safeGeneration, long pendingGeneration) {
        int indexOfRepo = -1;
        for (int i = 0; i < repositories.size(); i++) {
            if (repositories.get(i).name().equals(repoName)) {
                indexOfRepo = i;
                break;
            }
        }
        if (indexOfRepo < 0) {
            throw new IllegalArgumentException("Unknown repository [" + repoName + "]");
        }
        final List<RepositoryMetadata> updatedRepos = new ArrayList<>(repositories);
        updatedRepos.set(indexOfRepo, new RepositoryMetadata(repositories.get(indexOfRepo), safeGeneration, pendingGeneration));
        return new RepositoriesMetadata(updatedRepos);
    }

    /**
     * Returns list of currently registered repositories
     *
     * @return list of repositories
     */
    public List<RepositoryMetadata> repositories() {
        return this.repositories;
    }

    /**
     * Returns a repository with a given name or null if such repository doesn't exist
     *
     * @param name name of repository
     * @return repository metadata
     */
    public RepositoryMetadata repository(String name) {
        for (RepositoryMetadata repository : repositories) {
            if (name.equals(repository.name())) {
                return repository;
            }
        }
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RepositoriesMetadata that = (RepositoriesMetadata) o;

        return repositories.equals(that.repositories);
    }

    /**
     * Checks if this instance and the given instance share the same repositories by checking that this instances' repositories and the
     * repositories in {@code other} are equal or only differ in their values of {@link RepositoryMetadata#generation()} and
     * {@link RepositoryMetadata#pendingGeneration()}.
     *
     * @param other other repositories metadata
     * @return {@code true} iff both instances contain the same repositories apart from differences in generations
     */
    public boolean equalsIgnoreGenerations(@Nullable RepositoriesMetadata other) {
        if (other == null) {
            return false;
        }
        if (other.repositories.size() != repositories.size()) {
            return false;
        }
        for (int i = 0; i < repositories.size(); i++) {
            if (repositories.get(i).equalsIgnoreGenerations(other.repositories.get(i)) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Checks if this instance and the give instance share the same repositories, with option to skip checking for a list of repos.
     * This will support
     * @param other other repositories metadata
     * @param reposToSkip list of repos to skip check for equality
     * @return {@code true} iff both instances contain the same repositories apart from differences in generations, not including repos provided in reposToSkip.
     */
    public boolean equalsIgnoreGenerationsWithRepoSkip(@Nullable RepositoriesMetadata other, List<String> reposToSkip) {
        if (other == null) {
            return false;
        }
        List<RepositoryMetadata> currentRepositories = repositories.stream()
            .filter(repo -> !reposToSkip.contains(repo.name()))
            .collect(Collectors.toList());
        List<RepositoryMetadata> otherRepositories = other.repositories.stream()
            .filter(repo -> !reposToSkip.contains(repo.name()))
            .collect(Collectors.toList());

        return equalsRepository(currentRepositories, otherRepositories);
    }

    public boolean equalsIgnoreGenerationsForRepo(@Nullable RepositoriesMetadata other, List<String> reposToValidate) {
        if (other == null) {
            return false;
        }
        List<RepositoryMetadata> currentRepositories = repositories.stream()
            .filter(repo -> reposToValidate.contains(repo.name()))
            .collect(Collectors.toList());
        List<RepositoryMetadata> otherRepositories = other.repositories.stream()
            .filter(repo -> reposToValidate.contains(repo.name()))
            .collect(Collectors.toList());

        return equalsRepository(currentRepositories, otherRepositories);
    }

    public static boolean equalsRepository(List<RepositoryMetadata> currentRepositories, List<RepositoryMetadata> otherRepositories) {
        if (otherRepositories.size() != currentRepositories.size()) {
            return false;
        }
        // Sort repos by name for ordered comparison
        Comparator<RepositoryMetadata> compareByName = (o1, o2) -> o1.name().compareTo(o2.name());
        currentRepositories.sort(compareByName);
        otherRepositories.sort(compareByName);

        for (int i = 0; i < currentRepositories.size(); i++) {
            if (currentRepositories.get(i).equalsIgnoreGenerations(otherRepositories.get(i)) == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return repositories.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT.minimumCompatibilityVersion();
    }

    public RepositoriesMetadata(StreamInput in) throws IOException {
        this.repositories = in.readList(RepositoryMetadata::new);
    }

    public static NamedDiff<Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Custom.class, TYPE, in);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(repositories);
    }

    public static RepositoriesMetadata fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token;
        List<RepositoryMetadata> repository = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.START_OBJECT) {
                // move to next token if parsing the whole object
                token = parser.nextToken();
            }
            if (token == XContentParser.Token.FIELD_NAME) {
                String name = parser.currentName();
                if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                    throw new OpenSearchParseException("failed to parse repository [{}], expected object", name);
                }
                String type = null;
                Settings settings = Settings.EMPTY;
                long generation = RepositoryData.UNKNOWN_REPO_GEN;
                long pendingGeneration = RepositoryData.EMPTY_REPO_GEN;
                CryptoMetadata cryptoMetadata = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        String currentFieldName = parser.currentName();
                        if ("type".equals(currentFieldName)) {
                            if (parser.nextToken() != XContentParser.Token.VALUE_STRING) {
                                throw new OpenSearchParseException("failed to parse repository [{}], unknown type", name);
                            }
                            type = parser.text();
                        } else if ("settings".equals(currentFieldName)) {
                            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                                throw new OpenSearchParseException("failed to parse repository [{}], incompatible params", name);
                            }
                            settings = Settings.fromXContent(parser);
                        } else if ("generation".equals(currentFieldName)) {
                            if (parser.nextToken() != XContentParser.Token.VALUE_NUMBER) {
                                throw new OpenSearchParseException("failed to parse repository [{}], unknown type", name);
                            }
                            generation = parser.longValue();
                        } else if ("pending_generation".equals(currentFieldName)) {
                            if (parser.nextToken() != XContentParser.Token.VALUE_NUMBER) {
                                throw new OpenSearchParseException("failed to parse repository [{}], unknown type", name);
                            }
                            pendingGeneration = parser.longValue();
                        } else if (CryptoMetadata.CRYPTO_METADATA_KEY.equals(currentFieldName)) {
                            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                                throw new OpenSearchParseException("failed to parse repository [{}], unknown type", name);
                            }
                            cryptoMetadata = CryptoMetadata.fromXContent(parser);
                        } else {
                            throw new OpenSearchParseException(
                                "failed to parse repository [{}], unknown field [{}]",
                                name,
                                currentFieldName
                            );
                        }
                    } else {
                        throw new OpenSearchParseException("failed to parse repository [{}]", name);
                    }
                }
                if (type == null) {
                    throw new OpenSearchParseException("failed to parse repository [{}], missing repository type", name);
                }
                repository.add(new RepositoryMetadata(name, type, settings, generation, pendingGeneration, cryptoMetadata));
            } else {
                throw new OpenSearchParseException("failed to parse repositories");
            }
        }
        return new RepositoriesMetadata(repository);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        for (RepositoryMetadata repository : repositories) {
            toXContent(repository, builder, params);
        }
        return builder;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.API_AND_GATEWAY;
    }

    /**
     * Serializes information about a single repository
     *
     * @param repository repository metadata
     * @param builder    XContent builder
     * @param params     serialization parameters
     */
    public static void toXContent(RepositoryMetadata repository, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(repository.name());
        builder.field("type", repository.type());
        if (repository.cryptoMetadata() != null) {
            repository.cryptoMetadata().toXContent(repository.cryptoMetadata(), builder, params);
        }
        Settings settings = repository.settings();
        if (SYSTEM_REPOSITORY_SETTING.get(settings) && params.paramAsBoolean(HIDE_SYSTEM_REPOSITORY_SETTING, false)) {
            settings = repository.settings().filter(s -> !s.equals(SYSTEM_REPOSITORY_SETTING.getKey()));
        }
        builder.startObject("settings");
        settings.toXContent(builder, params);
        builder.endObject();

        if (params.paramAsBoolean(HIDE_GENERATIONS_PARAM, false) == false) {
            builder.field("generation", repository.generation());
            builder.field("pending_generation", repository.pendingGeneration());
        }
        builder.endObject();
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }
}
