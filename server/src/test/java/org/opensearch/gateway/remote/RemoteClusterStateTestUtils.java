/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.TestClusterStateCustom;
import org.opensearch.test.TestCustomMetadata;

import java.io.IOException;
import java.util.EnumSet;

public class RemoteClusterStateTestUtils {
    public static class CustomMetadata1 extends TestCustomMetadata {
        public static final String TYPE = "custom_md_1";

        public CustomMetadata1(String data) {
            super(data);
        }

        public CustomMetadata1(StreamInput in) throws IOException {
            super(in.readString());
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY);
        }
    }

    public static class CustomMetadata2 extends TestCustomMetadata {
        public static final String TYPE = "custom_md_2";

        public CustomMetadata2(String data) {
            super(data);
        }

        public CustomMetadata2(StreamInput in) throws IOException {
            super(in.readString());
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY);
        }
    }

    public static class CustomMetadata3 extends TestCustomMetadata {
        public static final String TYPE = "custom_md_3";

        public CustomMetadata3(String data) {
            super(data);
        }

        public CustomMetadata3(StreamInput in) throws IOException {
            super(in.readString());
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY);
        }
    }

    public static class CustomMetadata4 extends TestCustomMetadata {
        public static final String TYPE = "custom_md_4";

        public CustomMetadata4(String data) {
            super(data);
        }

        public CustomMetadata4(StreamInput in) throws IOException {
            super(in.readString());
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY);
        }
    }

    public static class CustomMetadata5 extends TestCustomMetadata {
        public static final String TYPE = "custom_md_5";

        public CustomMetadata5(String data) {
            super(data);
        }

        public CustomMetadata5(StreamInput in) throws IOException {
            super(in.readString());
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.API);
        }
    }

    public static class TestClusterStateCustom1 extends TestClusterStateCustom {

        public static final String TYPE = "custom_1";

        public TestClusterStateCustom1(String value) {
            super(value);
        }

        public TestClusterStateCustom1(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }
    }

    public static class TestClusterStateCustom2 extends TestClusterStateCustom {

        public static final String TYPE = "custom_2";

        public TestClusterStateCustom2(String value) {
            super(value);
        }

        public TestClusterStateCustom2(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }
    }

    public static class TestClusterStateCustom3 extends TestClusterStateCustom {

        public static final String TYPE = "custom_3";

        public TestClusterStateCustom3(String value) {
            super(value);
        }

        public TestClusterStateCustom3(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }
    }

    public static class TestClusterStateCustom4 extends TestClusterStateCustom {

        public static final String TYPE = "custom_4";

        public TestClusterStateCustom4(String value) {
            super(value);
        }

        public TestClusterStateCustom4(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }
    }
}
