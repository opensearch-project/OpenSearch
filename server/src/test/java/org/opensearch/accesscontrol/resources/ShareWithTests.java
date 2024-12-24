/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.accesscontrol.resources;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.mockito.Mockito;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ShareWithTests extends OpenSearchTestCase {

    public void testFromXContentWhenCurrentTokenIsNotStartObject() throws IOException {
        String json = "{\"read_only\": {\"users\": [\"user1\"], \"roles\": [], \"backend_roles\": []}}";
        XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, json);

        parser.nextToken();

        ShareWith shareWith = ShareWith.fromXContent(parser);

        assertNotNull(shareWith);
        Set<SharedWithScope> sharedWithScopes = shareWith.getSharedWithScopes();
        assertNotNull(sharedWithScopes);
        MatcherAssert.assertThat(1, equalTo(sharedWithScopes.size()));

        SharedWithScope scope = sharedWithScopes.iterator().next();
        MatcherAssert.assertThat("read_only", equalTo(scope.getScope()));

        SharedWithScope.ScopeRecipients scopeRecipients = scope.getSharedWithPerScope();
        assertNotNull(scopeRecipients);
        MatcherAssert.assertThat(1, equalTo(scopeRecipients.getUsers().size()));
        MatcherAssert.assertThat("user1", equalTo(scopeRecipients.getUsers().iterator().next()));
        MatcherAssert.assertThat(0, equalTo(scopeRecipients.getRoles().size()));
        MatcherAssert.assertThat(0, equalTo(scopeRecipients.getBackendRoles().size()));
    }

    public void testFromXContentWithEmptyInput() throws IOException {
        String emptyJson = "{}";
        XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry(), null, emptyJson);

        ShareWith result = ShareWith.fromXContent(parser);

        assertNotNull(result);
        MatcherAssert.assertThat(result.getSharedWithScopes(), is(empty()));
    }

    public void testFromXContentWithStartObject() throws IOException {
        XContentParser parser;
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject()
                .startObject(ResourceAccessScope.READ_ONLY)
                .array("users", "user1", "user2")
                .array("roles", "role1")
                .array("backend_roles", "backend_role1")
                .endObject()
                .startObject(ResourceAccessScope.READ_WRITE)
                .array("users", "user3")
                .array("roles", "role2", "role3")
                .array("backend_roles")
                .endObject()
                .endObject();

            parser = JsonXContent.jsonXContent.createParser(null, null, builder.toString());
        }

        parser.nextToken();

        ShareWith shareWith = ShareWith.fromXContent(parser);

        assertNotNull(shareWith);
        Set<SharedWithScope> scopes = shareWith.getSharedWithScopes();
        MatcherAssert.assertThat(scopes.size(), equalTo(2));

        for (SharedWithScope scope : scopes) {
            SharedWithScope.ScopeRecipients perScope = scope.getSharedWithPerScope();
            if (scope.getScope().equals(ResourceAccessScope.READ_ONLY)) {
                MatcherAssert.assertThat(perScope.getUsers().size(), equalTo(2));
                MatcherAssert.assertThat(perScope.getRoles().size(), equalTo(1));
                MatcherAssert.assertThat(perScope.getBackendRoles().size(), equalTo(1));
            } else if (scope.getScope().equals(ResourceAccessScope.READ_WRITE)) {
                MatcherAssert.assertThat(perScope.getUsers().size(), equalTo(1));
                MatcherAssert.assertThat(perScope.getRoles().size(), equalTo(2));
                MatcherAssert.assertThat(perScope.getBackendRoles().size(), equalTo(0));
            }
        }
    }

    public void testFromXContentWithUnexpectedEndOfInput() throws IOException {
        XContentParser mockParser = mock(XContentParser.class);
        when(mockParser.currentToken()).thenReturn(XContentParser.Token.START_OBJECT);
        when(mockParser.nextToken()).thenReturn(XContentParser.Token.END_OBJECT, (XContentParser.Token) null);

        ShareWith result = ShareWith.fromXContent(mockParser);

        assertNotNull(result);
        MatcherAssert.assertThat(result.getSharedWithScopes(), is(empty()));
    }

    public void testToXContentBuildsCorrectly() throws IOException {
        SharedWithScope scope = new SharedWithScope("scope1", new SharedWithScope.ScopeRecipients(Set.of(), Set.of(), Set.of()));

        Set<SharedWithScope> scopes = new HashSet<>();
        scopes.add(scope);

        ShareWith shareWith = new ShareWith(scopes);

        XContentBuilder builder = JsonXContent.contentBuilder();

        shareWith.toXContent(builder, null);

        String result = builder.toString();

        String expected = "{\"scope1\":{\"users\":[],\"roles\":[],\"backend_roles\":[]}}";

        MatcherAssert.assertThat(expected.length(), equalTo(result.length()));
        MatcherAssert.assertThat(expected, equalTo(result));
    }

    public void testWriteToWithEmptySet() throws IOException {
        Set<SharedWithScope> emptySet = Collections.emptySet();
        ShareWith shareWith = new ShareWith(emptySet);
        StreamOutput mockOutput = Mockito.mock(StreamOutput.class);

        shareWith.writeTo(mockOutput);

        verify(mockOutput).writeCollection(emptySet);
    }

    public void testWriteToWithIOException() throws IOException {
        Set<SharedWithScope> set = new HashSet<>();
        set.add(new SharedWithScope("test", new SharedWithScope.ScopeRecipients(Set.of(), Set.of(), Set.of())));
        ShareWith shareWith = new ShareWith(set);
        StreamOutput mockOutput = Mockito.mock(StreamOutput.class);

        doThrow(new IOException("Simulated IO exception")).when(mockOutput).writeCollection(set);

        assertThrows(IOException.class, () -> shareWith.writeTo(mockOutput));
    }

    public void testWriteToWithLargeSet() throws IOException {
        Set<SharedWithScope> largeSet = new HashSet<>();
        for (int i = 0; i < 10000; i++) {
            largeSet.add(new SharedWithScope("scope" + i, new SharedWithScope.ScopeRecipients(Set.of(), Set.of(), Set.of())));
        }
        ShareWith shareWith = new ShareWith(largeSet);
        StreamOutput mockOutput = Mockito.mock(StreamOutput.class);

        shareWith.writeTo(mockOutput);

        verify(mockOutput).writeCollection(largeSet);
    }

    public void test_fromXContent_emptyObject() throws IOException {
        XContentParser parser;
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject().endObject();
            parser = XContentType.JSON.xContent().createParser(null, null, builder.toString());
        }

        ShareWith shareWith = ShareWith.fromXContent(parser);

        MatcherAssert.assertThat(shareWith.getSharedWithScopes(), is(empty()));
    }

    public void test_writeSharedWithScopesToStream() throws IOException {
        StreamOutput mockStreamOutput = Mockito.mock(StreamOutput.class);

        Set<SharedWithScope> sharedWithScopes = new HashSet<>();
        sharedWithScopes.add(
            new SharedWithScope(ResourceAccessScope.READ_ONLY, new SharedWithScope.ScopeRecipients(Set.of(), Set.of(), Set.of()))
        );
        sharedWithScopes.add(
            new SharedWithScope(ResourceAccessScope.READ_WRITE, new SharedWithScope.ScopeRecipients(Set.of(), Set.of(), Set.of()))
        );

        ShareWith shareWith = new ShareWith(sharedWithScopes);

        shareWith.writeTo(mockStreamOutput);

        verify(mockStreamOutput, times(1)).writeCollection(eq(sharedWithScopes));
    }

}
