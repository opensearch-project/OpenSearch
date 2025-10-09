/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.common;

import org.opensearch.core.common.Strings;
import org.opensearch.protobufs.BulkRequest;
import org.opensearch.protobufs.SearchRequest;
import org.opensearch.protobufs.SourceConfig;
import org.opensearch.protobufs.SourceConfigParam;
import org.opensearch.protobufs.SourceFilter;
import org.opensearch.protobufs.StringArray;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.test.OpenSearchTestCase;

public class FetchSourceContextProtoUtilsTests extends OpenSearchTestCase {

    public void testParseFromProtoRequestWithBoolValue() {
        // Create a BulkRequest with source as boolean
        BulkRequest request = BulkRequest.newBuilder().setXSource(SourceConfigParam.newBuilder().setBool(true).build()).build();

        // Parse the request
        FetchSourceContext context = FetchSourceContextProtoUtils.parseFromProtoRequest(request);

        // Verify the result
        assertNotNull("Context should not be null", context);
        assertTrue("fetchSource should be true", context.fetchSource());
        assertArrayEquals("includes should be empty", Strings.EMPTY_ARRAY, context.includes());
        assertArrayEquals("excludes should be empty", Strings.EMPTY_ARRAY, context.excludes());
    }

    public void testParseFromProtoRequestWithStringArray() {
        // Create a BulkRequest with source as string array
        BulkRequest request = BulkRequest.newBuilder()
            .setXSource(
                SourceConfigParam.newBuilder()
                    .setStringArray(StringArray.newBuilder().addStringArray("field1").addStringArray("field2").build())
                    .build()
            )
            .build();

        // Parse the request
        FetchSourceContext context = FetchSourceContextProtoUtils.parseFromProtoRequest(request);

        // Verify the result
        assertNotNull("Context should not be null", context);
        assertTrue("fetchSource should be true", context.fetchSource());
        assertArrayEquals("includes should match", new String[] { "field1", "field2" }, context.includes());
        assertArrayEquals("excludes should be empty", Strings.EMPTY_ARRAY, context.excludes());
    }

    public void testParseFromProtoRequestWithSourceIncludes() {
        // Create a BulkRequest with source includes
        BulkRequest request = BulkRequest.newBuilder().addXSourceIncludes("field1").addXSourceIncludes("field2").build();

        // Parse the request
        FetchSourceContext context = FetchSourceContextProtoUtils.parseFromProtoRequest(request);

        // Verify the result
        assertNotNull("Context should not be null", context);
        assertTrue("fetchSource should be true", context.fetchSource());
        assertArrayEquals("includes should match", new String[] { "field1", "field2" }, context.includes());
        assertArrayEquals("excludes should be empty", Strings.EMPTY_ARRAY, context.excludes());
    }

    public void testParseFromProtoRequestWithSourceExcludes() {
        // Create a BulkRequest with source excludes
        BulkRequest request = BulkRequest.newBuilder().addXSourceExcludes("field1").addXSourceExcludes("field2").build();

        // Parse the request
        FetchSourceContext context = FetchSourceContextProtoUtils.parseFromProtoRequest(request);

        // Verify the result
        assertNotNull("Context should not be null", context);
        assertTrue("fetchSource should be true", context.fetchSource());
        assertArrayEquals("includes should be empty", Strings.EMPTY_ARRAY, context.includes());
        assertArrayEquals("excludes should match", new String[] { "field1", "field2" }, context.excludes());
    }

    public void testParseFromProtoRequestWithBothIncludesAndExcludes() {
        // Create a BulkRequest with both source includes and excludes
        BulkRequest request = BulkRequest.newBuilder()
            .addXSourceIncludes("include1")
            .addXSourceIncludes("include2")
            .addXSourceExcludes("exclude1")
            .addXSourceExcludes("exclude2")
            .build();

        // Parse the request
        FetchSourceContext context = FetchSourceContextProtoUtils.parseFromProtoRequest(request);

        // Verify the result
        assertNotNull("Context should not be null", context);
        assertTrue("fetchSource should be true", context.fetchSource());
        assertArrayEquals("includes should match", new String[] { "include1", "include2" }, context.includes());
        assertArrayEquals("excludes should match", new String[] { "exclude1", "exclude2" }, context.excludes());
    }

    public void testParseFromProtoRequestWithNoSourceParams() {
        // Create a BulkRequest with no source parameters
        BulkRequest request = BulkRequest.newBuilder().build();

        // Parse the request
        FetchSourceContext context = FetchSourceContextProtoUtils.parseFromProtoRequest(request);

        // Verify the result
        // The implementation returns a default FetchSourceContext with fetchSource=true
        // and empty includes/excludes arrays when no source parameters are provided
        assertNotNull("Context should not be null", context);
        assertTrue("fetchSource should be true", context.fetchSource());
        assertArrayEquals("includes should be empty", Strings.EMPTY_ARRAY, context.includes());
        assertArrayEquals("excludes should be empty", Strings.EMPTY_ARRAY, context.excludes());
    }

    public void testFromProtoWithFetch() {
        // Create a SourceConfig with fetch=true
        SourceConfig sourceConfig = SourceConfig.newBuilder().setFetch(true).build();

        // Convert to FetchSourceContext
        FetchSourceContext context = FetchSourceContextProtoUtils.fromProto(sourceConfig);

        // Verify the result
        assertNotNull("Context should not be null", context);
        assertTrue("fetchSource should be true", context.fetchSource());
        assertArrayEquals("includes should be empty", Strings.EMPTY_ARRAY, context.includes());
        assertArrayEquals("excludes should be empty", Strings.EMPTY_ARRAY, context.excludes());
    }

    public void testFromProtoWithIncludes() {
        // Create a SourceConfig with includes
        SourceConfig sourceConfig = SourceConfig.newBuilder()
            .setFilter(SourceFilter.newBuilder().addIncludes("field1").addIncludes("field2").build())
            .build();

        // Convert to FetchSourceContext
        FetchSourceContext context = FetchSourceContextProtoUtils.fromProto(sourceConfig);

        // Verify the result
        assertNotNull("Context should not be null", context);
        assertTrue("fetchSource should be true", context.fetchSource());
        assertArrayEquals("includes should match", new String[] { "field1", "field2" }, context.includes());
        assertArrayEquals("excludes should be empty", Strings.EMPTY_ARRAY, context.excludes());
    }

    public void testFromProtoWithFilterIncludes() {
        // Create a SourceConfig with filter includes
        SourceConfig sourceConfig = SourceConfig.newBuilder()
            .setFilter(SourceFilter.newBuilder().addIncludes("field1").addIncludes("field2").build())
            .build();

        // Convert to FetchSourceContext
        FetchSourceContext context = FetchSourceContextProtoUtils.fromProto(sourceConfig);

        // Verify the result
        assertNotNull("Context should not be null", context);
        assertTrue("fetchSource should be true", context.fetchSource());
        assertArrayEquals("includes should match", new String[] { "field1", "field2" }, context.includes());
        assertArrayEquals("excludes should be empty", Strings.EMPTY_ARRAY, context.excludes());
    }

    public void testFromProtoWithFilterExcludes() {
        // Create a SourceConfig with filter excludes
        SourceConfig sourceConfig = SourceConfig.newBuilder()
            .setFilter(SourceFilter.newBuilder().addExcludes("field1").addExcludes("field2").build())
            .build();

        // Convert to FetchSourceContext
        FetchSourceContext context = FetchSourceContextProtoUtils.fromProto(sourceConfig);

        // Verify the result
        assertNotNull("Context should not be null", context);
        assertTrue("fetchSource should be true", context.fetchSource());
        assertArrayEquals("includes should be empty", Strings.EMPTY_ARRAY, context.includes());
        assertArrayEquals("excludes should match", new String[] { "field1", "field2" }, context.excludes());
    }

    public void testParseFromProtoRequestWithSearchRequestBoolValue() {
        // Create a SearchRequest with source as boolean
        SearchRequest request = SearchRequest.newBuilder().setXSource(SourceConfigParam.newBuilder().setBool(true).build()).build();

        // Parse the request
        FetchSourceContext context = FetchSourceContextProtoUtils.parseFromProtoRequest(request);

        // Verify the result
        assertNotNull("Context should not be null", context);
        assertTrue("fetchSource should be true", context.fetchSource());
        assertArrayEquals("includes should be empty", Strings.EMPTY_ARRAY, context.includes());
        assertArrayEquals("excludes should be empty", Strings.EMPTY_ARRAY, context.excludes());
    }

    public void testParseFromProtoRequestWithSearchRequestStringArray() {
        // Create a SearchRequest with source as string array
        SearchRequest request = SearchRequest.newBuilder()
            .setXSource(
                SourceConfigParam.newBuilder()
                    .setStringArray(StringArray.newBuilder().addStringArray("field1").addStringArray("field2").build())
                    .build()
            )
            .build();

        // Parse the request
        FetchSourceContext context = FetchSourceContextProtoUtils.parseFromProtoRequest(request);

        // Verify the result
        assertNotNull("Context should not be null", context);
        assertTrue("fetchSource should be true", context.fetchSource());
        assertArrayEquals("includes should match", new String[] { "field1", "field2" }, context.includes());
        assertArrayEquals("excludes should be empty", Strings.EMPTY_ARRAY, context.excludes());
    }

    public void testParseFromProtoRequestWithSearchRequestSourceIncludes() {
        // Create a SearchRequest with source includes
        SearchRequest request = SearchRequest.newBuilder().addXSourceIncludes("field1").addXSourceIncludes("field2").build();

        // Parse the request
        FetchSourceContext context = FetchSourceContextProtoUtils.parseFromProtoRequest(request);

        // Verify the result
        assertNotNull("Context should not be null", context);
        assertTrue("fetchSource should be true", context.fetchSource());
        assertArrayEquals("includes should match", new String[] { "field1", "field2" }, context.includes());
        assertArrayEquals("excludes should be empty", Strings.EMPTY_ARRAY, context.excludes());
    }

    public void testParseFromProtoRequestWithSearchRequestSourceExcludes() {
        // Create a SearchRequest with source excludes
        SearchRequest request = SearchRequest.newBuilder().addXSourceExcludes("field1").addXSourceExcludes("field2").build();

        // Parse the request
        FetchSourceContext context = FetchSourceContextProtoUtils.parseFromProtoRequest(request);

        // Verify the result
        assertNotNull("Context should not be null", context);
        assertTrue("fetchSource should be true", context.fetchSource());
        assertArrayEquals("includes should be empty", Strings.EMPTY_ARRAY, context.includes());
        assertArrayEquals("excludes should match", new String[] { "field1", "field2" }, context.excludes());
    }

    public void testParseFromProtoRequestWithSearchRequestBothIncludesAndExcludes() {
        // Create a SearchRequest with both source includes and excludes
        SearchRequest request = SearchRequest.newBuilder()
            .addXSourceIncludes("include1")
            .addXSourceIncludes("include2")
            .addXSourceExcludes("exclude1")
            .addXSourceExcludes("exclude2")
            .build();

        // Parse the request
        FetchSourceContext context = FetchSourceContextProtoUtils.parseFromProtoRequest(request);

        // Verify the result
        assertNotNull("Context should not be null", context);
        assertTrue("fetchSource should be true", context.fetchSource());
        assertArrayEquals("includes should match", new String[] { "include1", "include2" }, context.includes());
        assertArrayEquals("excludes should match", new String[] { "exclude1", "exclude2" }, context.excludes());
    }

    public void testParseFromProtoRequestWithSearchRequestNoSourceParams() {
        // Create a SearchRequest with no source parameters
        SearchRequest request = SearchRequest.newBuilder().build();

        // Parse the request
        FetchSourceContext context = FetchSourceContextProtoUtils.parseFromProtoRequest(request);

        // Verify the result
        assertNull("Context should be null", context);
    }

    public void testFromProtoWithSourceConfigFetch() {
        // Create a SourceConfig with fetch=false
        SourceConfig sourceConfig = SourceConfig.newBuilder().setFetch(false).build();

        // Convert to FetchSourceContext
        FetchSourceContext context = FetchSourceContextProtoUtils.fromProto(sourceConfig);

        // Verify the result
        assertNotNull("Context should not be null", context);
        assertFalse("fetchSource should be false", context.fetchSource());
        assertArrayEquals("includes should be empty", Strings.EMPTY_ARRAY, context.includes());
        assertArrayEquals("excludes should be empty", Strings.EMPTY_ARRAY, context.excludes());
    }

    public void testFromProtoWithSourceConfigIncludes() {
        // Create a SourceConfig with includes
        SourceConfig sourceConfig = SourceConfig.newBuilder()
            .setFilter(SourceFilter.newBuilder().addIncludes("field1").addIncludes("field2").build())
            .build();

        // Convert to FetchSourceContext
        FetchSourceContext context = FetchSourceContextProtoUtils.fromProto(sourceConfig);

        // Verify the result
        assertNotNull("Context should not be null", context);
        assertTrue("fetchSource should be true", context.fetchSource());
        assertArrayEquals("includes should match", new String[] { "field1", "field2" }, context.includes());
        assertArrayEquals("excludes should be empty", Strings.EMPTY_ARRAY, context.excludes());
    }

    public void testFromProtoWithSourceConfigFilterIncludesOnly() {
        // Create a SourceConfig with filter includes only
        SourceConfig sourceConfig = SourceConfig.newBuilder()
            .setFilter(SourceFilter.newBuilder().addIncludes("field1").addIncludes("field2").build())
            .build();

        // Convert to FetchSourceContext
        FetchSourceContext context = FetchSourceContextProtoUtils.fromProto(sourceConfig);

        // Verify the result
        assertNotNull("Context should not be null", context);
        assertTrue("fetchSource should be true", context.fetchSource());
        assertArrayEquals("includes should match", new String[] { "field1", "field2" }, context.includes());
        assertArrayEquals("excludes should be empty", Strings.EMPTY_ARRAY, context.excludes());
    }

    public void testFromProtoWithSourceConfigFilterExcludesOnly() {
        // Create a SourceConfig with filter excludes only
        SourceConfig sourceConfig = SourceConfig.newBuilder()
            .setFilter(SourceFilter.newBuilder().addExcludes("field1").addExcludes("field2").build())
            .build();

        // Convert to FetchSourceContext
        FetchSourceContext context = FetchSourceContextProtoUtils.fromProto(sourceConfig);

        // Verify the result
        assertNotNull("Context should not be null", context);
        assertTrue("fetchSource should be true", context.fetchSource());
        assertArrayEquals("includes should be empty", Strings.EMPTY_ARRAY, context.includes());
        assertArrayEquals("excludes should match", new String[] { "field1", "field2" }, context.excludes());
    }

    public void testFromProtoWithSourceConfigFilterBothIncludesAndExcludes() {
        // Create a SourceConfig with filter includes and excludes
        SourceConfig sourceConfig = SourceConfig.newBuilder()
            .setFilter(
                SourceFilter.newBuilder()
                    .addIncludes("include1")
                    .addIncludes("include2")
                    .addExcludes("exclude1")
                    .addExcludes("exclude2")
                    .build()
            )
            .build();

        // Convert to FetchSourceContext
        FetchSourceContext context = FetchSourceContextProtoUtils.fromProto(sourceConfig);

        // Verify the result
        assertNotNull("Context should not be null", context);
        assertTrue("fetchSource should be true", context.fetchSource());
        assertArrayEquals("includes should match", new String[] { "include1", "include2" }, context.includes());
        assertArrayEquals("excludes should match", new String[] { "exclude1", "exclude2" }, context.excludes());
    }
}
