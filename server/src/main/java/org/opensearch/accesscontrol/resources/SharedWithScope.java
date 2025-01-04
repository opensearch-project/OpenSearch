/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.accesscontrol.resources;

import org.opensearch.core.common.io.stream.NamedWriteable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This class represents the scope at which a resource is shared with.
 * Example:
 * "read_only": {
 *      "users": [],
 *      "roles": [],
 *      "backend_roles": []
 * }
 * where "users", "roles" and "backend_roles" are the recipient entities
 *
 * @opensearch.experimental
 */
public class SharedWithScope implements ToXContentFragment, NamedWriteable {

    private final String scope;

    private final ScopeRecipients scopeRecipients;

    public SharedWithScope(String scope, ScopeRecipients scopeRecipients) {
        this.scope = scope;
        this.scopeRecipients = scopeRecipients;
    }

    public SharedWithScope(StreamInput in) throws IOException {
        this.scope = in.readString();
        this.scopeRecipients = new ScopeRecipients(in);
    }

    public String getScope() {
        return scope;
    }

    public ScopeRecipients getSharedWithPerScope() {
        return scopeRecipients;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(scope);
        builder.startObject();

        scopeRecipients.toXContent(builder, params);

        return builder.endObject();
    }

    public static SharedWithScope fromXContent(XContentParser parser) throws IOException {
        String scope = parser.currentName();

        parser.nextToken();

        ScopeRecipients scopeRecipients = ScopeRecipients.fromXContent(parser);

        return new SharedWithScope(scope, scopeRecipients);
    }

    @Override
    public String toString() {
        return "{" + scope + ": " + scopeRecipients + '}';
    }

    @Override
    public String getWriteableName() {
        return "shared_with_scope";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(scope);
        out.writeNamedWriteable(scopeRecipients);
    }

    /**
     * This class represents the entities with whom a resource is shared with for a given scope.
     *
     * @opensearch.experimental
     */
    public static class ScopeRecipients implements ToXContentFragment, NamedWriteable {

        private final Map<RecipientType, Set<String>> recipients;

        public ScopeRecipients(Map<RecipientType, Set<String>> recipients) {
            if (recipients == null) {
                throw new IllegalArgumentException("Recipients map cannot be null");
            }
            this.recipients = recipients;
        }

        public ScopeRecipients(StreamInput in) throws IOException {
            this.recipients = in.readMap(
                key -> RecipientTypeRegistry.fromValue(key.readString()),
                input -> input.readSet(StreamInput::readString)
            );
        }

        public Map<RecipientType, Set<String>> getRecipients() {
            return recipients;
        }

        @Override
        public String getWriteableName() {
            return "scope_recipients";
        }

        public static ScopeRecipients fromXContent(XContentParser parser) throws IOException {
            Map<RecipientType, Set<String>> recipients = new HashMap<>();

            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String fieldName = parser.currentName();
                    RecipientType recipientType = RecipientTypeRegistry.fromValue(fieldName);

                    parser.nextToken();
                    Set<String> values = new HashSet<>();
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        values.add(parser.text());
                    }
                    recipients.put(recipientType, values);
                }
            }

            return new ScopeRecipients(recipients);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(
                recipients,
                (streamOutput, recipientType) -> streamOutput.writeString(recipientType.getType()),
                (streamOutput, strings) -> streamOutput.writeCollection(strings, StreamOutput::writeString)
            );
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (recipients.isEmpty()) {
                return builder;
            }
            for (Map.Entry<RecipientType, Set<String>> entry : recipients.entrySet()) {
                builder.array(entry.getKey().getType(), entry.getValue().toArray());
            }
            return builder;
        }
    }
}
