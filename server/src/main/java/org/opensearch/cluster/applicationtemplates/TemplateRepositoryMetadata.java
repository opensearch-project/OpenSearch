/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.applicationtemplates;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Objects;

/**
 * The information to uniquely identify a template repository.
 */
@ExperimentalApi
public class TemplateRepositoryMetadata {

    private final String id;
    private final long version;

    public TemplateRepositoryMetadata(String id, long version) {
        this.id = id;
        this.version = version;
    }

    public String id() {
        return id;
    }

    public long version() {
        return version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TemplateRepositoryMetadata that = (TemplateRepositoryMetadata) o;
        return version == that.version && Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, version);
    }

    @Override
    public String toString() {
        return "TemplateRepositoryMetadata{" +
            "id='" + id + '\'' +
            ", version=" + version +
            '}';
    }
}
