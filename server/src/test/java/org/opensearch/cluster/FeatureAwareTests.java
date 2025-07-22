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

package org.opensearch.cluster;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Optional;

import static org.opensearch.test.VersionUtils.randomVersionBetween;

public class FeatureAwareTests extends OpenSearchTestCase {

    abstract static class Custom implements Metadata.Custom {

        private final Version version;

        Custom(final Version version) {
            this.version = version;
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return Metadata.ALL_CONTEXTS;
        }

        @Override
        public Diff<Metadata.Custom> diff(final Metadata.Custom previousState) {
            return null;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {

        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            return builder;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return version;
        }

    }

    static class NoRequiredFeatureCustom extends Custom {

        NoRequiredFeatureCustom(final Version version) {
            super(version);
        }

        @Override
        public String getWriteableName() {
            return "no-required-feature";
        }

    }

    static class RequiredFeatureCustom extends Custom {

        RequiredFeatureCustom(final Version version) {
            super(version);
        }

        @Override
        public String getWriteableName() {
            return null;
        }

        @Override
        public Optional<String> getRequiredFeature() {
            return Optional.of("required-feature");
        }

    }

    public void testVersion() {
        final Version version = randomValueOtherThan(VersionUtils.getFirstVersion(), () -> VersionUtils.randomVersion(random()));
        for (final Custom custom : Arrays.asList(new NoRequiredFeatureCustom(version), new RequiredFeatureCustom(version))) {
            {
                final BytesStreamOutput out = new BytesStreamOutput();
                final Version afterVersion = randomVersionBetween(random(), version, Version.CURRENT);
                out.setVersion(afterVersion);
                if (custom.getRequiredFeature().isPresent()) {
                    out.setFeatures(Collections.singleton(custom.getRequiredFeature().get()));
                }
                assertTrue(FeatureAware.shouldSerialize(out, custom));
            }
            {
                final BytesStreamOutput out = new BytesStreamOutput();
                final Version beforeVersion = randomVersionBetween(
                    random(),
                    VersionUtils.getFirstVersion(),
                    VersionUtils.getPreviousVersion(version)
                );
                out.setVersion(beforeVersion);
                if (custom.getRequiredFeature().isPresent() && randomBoolean()) {
                    out.setFeatures(Collections.singleton(custom.getRequiredFeature().get()));
                }
                assertFalse(FeatureAware.shouldSerialize(out, custom));
            }
        }
    }

    public void testFeature() {
        final Version version = VersionUtils.randomVersion(random());
        final Version afterVersion = randomVersionBetween(random(), version, Version.CURRENT);
        final Custom custom = new RequiredFeatureCustom(version);
        // the feature is present
        final BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(afterVersion);
        assertTrue(custom.getRequiredFeature().isPresent());
        out.setFeatures(Collections.singleton(custom.getRequiredFeature().get()));
        assertTrue(FeatureAware.shouldSerialize(out, custom));
    }

    public void testMissingFeature() {
        final Version version = VersionUtils.randomVersion(random());
        final Version afterVersion = randomVersionBetween(random(), version, Version.CURRENT);
        final Custom custom = new RequiredFeatureCustom(version);
        // the feature is missing
        final BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(afterVersion);
        assertTrue(FeatureAware.shouldSerialize(out, custom));
    }
}
