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
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;
import org.hamcrest.MatcherAssert;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.mockito.Mockito;

import static org.hamcrest.Matchers.contains;
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

    @Before
    public void setupResourceRecipientTypes() {
        initializeRecipientTypes();
    }

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
        Map<RecipientType, Set<String>> recipients = scopeRecipients.getRecipients();
        MatcherAssert.assertThat(recipients.get(RecipientTypeRegistry.fromValue(DefaultRecipientType.USERS.getName())).size(), is(1));
        MatcherAssert.assertThat(recipients.get(RecipientTypeRegistry.fromValue(DefaultRecipientType.USERS.getName())), contains("user1"));
        MatcherAssert.assertThat(recipients.get(RecipientTypeRegistry.fromValue(DefaultRecipientType.ROLES.getName())).size(), is(0));
        MatcherAssert.assertThat(
            recipients.get(RecipientTypeRegistry.fromValue(DefaultRecipientType.BACKEND_ROLES.getName())).size(),
            is(0)
        );
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
            Map<RecipientType, Set<String>> recipients = perScope.getRecipients();
            if (scope.getScope().equals(ResourceAccessScope.READ_ONLY)) {
                MatcherAssert.assertThat(
                    recipients.get(RecipientTypeRegistry.fromValue(DefaultRecipientType.USERS.getName())).size(),
                    is(2)
                );
                MatcherAssert.assertThat(
                    recipients.get(RecipientTypeRegistry.fromValue(DefaultRecipientType.ROLES.getName())).size(),
                    is(1)
                );
                MatcherAssert.assertThat(
                    recipients.get(RecipientTypeRegistry.fromValue(DefaultRecipientType.BACKEND_ROLES.getName())).size(),
                    is(1)
                );
            } else if (scope.getScope().equals(ResourceAccessScope.READ_WRITE)) {
                MatcherAssert.assertThat(
                    recipients.get(RecipientTypeRegistry.fromValue(DefaultRecipientType.USERS.getName())).size(),
                    is(1)
                );
                MatcherAssert.assertThat(
                    recipients.get(RecipientTypeRegistry.fromValue(DefaultRecipientType.ROLES.getName())).size(),
                    is(2)
                );
                MatcherAssert.assertThat(
                    recipients.get(RecipientTypeRegistry.fromValue(DefaultRecipientType.BACKEND_ROLES.getName())).size(),
                    is(0)
                );
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
        SharedWithScope scope = new SharedWithScope(
            "scope1",
            new SharedWithScope.ScopeRecipients(Map.of(new RecipientType("users"), Set.of("bleh")))
        );

        Set<SharedWithScope> scopes = new HashSet<>();
        scopes.add(scope);

        ShareWith shareWith = new ShareWith(scopes);

        XContentBuilder builder = JsonXContent.contentBuilder();

        shareWith.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String result = builder.toString();

        String expected = "{\"scope1\":{\"users\":[\"bleh\"]}}";

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
        set.add(new SharedWithScope("test", new SharedWithScope.ScopeRecipients(Map.of())));
        ShareWith shareWith = new ShareWith(set);
        StreamOutput mockOutput = Mockito.mock(StreamOutput.class);

        doThrow(new IOException("Simulated IO exception")).when(mockOutput).writeCollection(set);

        assertThrows(IOException.class, () -> shareWith.writeTo(mockOutput));
    }

    public void testWriteToWithLargeSet() throws IOException {
        Set<SharedWithScope> largeSet = new HashSet<>();
        for (int i = 0; i < 10000; i++) {
            largeSet.add(new SharedWithScope("scope" + i, new SharedWithScope.ScopeRecipients(Map.of())));
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
        sharedWithScopes.add(new SharedWithScope(ResourceAccessScope.READ_ONLY, new SharedWithScope.ScopeRecipients(Map.of())));
        sharedWithScopes.add(new SharedWithScope(ResourceAccessScope.READ_WRITE, new SharedWithScope.ScopeRecipients(Map.of())));

        ShareWith shareWith = new ShareWith(sharedWithScopes);

        shareWith.writeTo(mockStreamOutput);

        verify(mockStreamOutput, times(1)).writeCollection(eq(sharedWithScopes));
    }

    private void initializeRecipientTypes() {
        RecipientTypeRegistry.registerRecipientType("users", new RecipientType("users"));
        RecipientTypeRegistry.registerRecipientType("roles", new RecipientType("roles"));
        RecipientTypeRegistry.registerRecipientType("backend_roles", new RecipientType("backend_roles"));
    }
}

enum DefaultRecipientType {
    USERS("users"),
    ROLES("roles"),
    BACKEND_ROLES("backend_roles");

    private final String name;

    DefaultRecipientType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
