/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.remotecluster;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.identity.IdentityConfigConstants;
import org.opensearch.identity.rest.IdentityRestConstants;
import org.opensearch.identity.utils.ErrorType;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.util.List;
import java.util.Map;

/**
 * Tests REST API for users against remote cluster
 */
public class UserIT extends IdentityRestTestCase {

    @SuppressWarnings("unchecked")
    public void testInternalUsersApi() throws Exception {

        final Map<String, String> emptyMap = Map.of();
        final List<String> emptyList = List.of();

        String username = "test-create";
        String createContent = "{ \"password\" : \"test\","
            + " \"attributes\": { \"attribute1\": \"value1\"},"
            + " \"permissions\": [\"indices:admin:create\"]"
            + " }\n";

        String updateContent = "{ \"password\" : \"test\","
            + " \"attributes\": { \"attribute1\": \"value2\"},"
            + " \"permissions\": [\"indices:admin:update\"]"
            + " }\n";

        Map<String, String> expectedAttributes = Map.of("attribute1", "value2");
        List<String> expectedPermissions = List.of("indices:admin:create");

        // Create a user
        String createMessage = username + " created successfully.";
        Request request = new Request("PUT", ENDPOINT + "/users/" + username);
        request.setJsonEntity(createContent);
        request.setOptions(options());
        Response response = client().performRequest(request);
        assertEquals(response.getStatusLine().getStatusCode(), 200);
        Map<String, Object> userCreated = entityAsMap(response);
        assertEquals(userCreated.size(), 3);
        assertEquals(userCreated.get("successful"), true);
        assertEquals(userCreated.get("username"), username);
        assertEquals(userCreated.get("message"), createMessage);

        // Update a user
        String updateMessage = username + " updated successfully.";
        request = new Request("PUT", ENDPOINT + "/users/" + username);
        request.setJsonEntity(updateContent);
        request.setOptions(options());
        response = client().performRequest(request);
        assertEquals(response.getStatusLine().getStatusCode(), 200);
        Map<String, Object> userUpdated = entityAsMap(response);
        assertEquals(userUpdated.size(), 3);
        assertEquals(userUpdated.get("successful"), true);
        assertEquals(userUpdated.get("username"), username);
        assertEquals(userUpdated.get("message"), updateMessage);

        // Get a user
        Request getRequest = new Request("GET", ENDPOINT + "/users/" + username);
        request.setOptions(options());
        response = client().performRequest(getRequest);
        assertEquals(response.getStatusLine().getStatusCode(), 200);
        Map<String, Object> getResponse = entityAsMap(response);
        Map<String, String> user = (Map<String, String>) getResponse.get(username);
        assertNotEquals(user, null);
        assertEquals(user.get("attributes"), expectedAttributes);
        assertEquals(user.get("permissions"), expectedPermissions);

        // Get all users
        Request mGetRequest = new Request("GET", ENDPOINT + "/users");
        request.setOptions(options());
        response = client().performRequest(mGetRequest);
        assertEquals(response.getStatusLine().getStatusCode(), 200);
        Map<String, Object> mGetResponse = entityAsMap(response);
        List<Map<String, Object>> users = (List<Map<String, Object>>) mGetResponse.get("users");
        assertEquals(users.size(), 11); // Refer: distribution/src/config/internal_users.yml

        // Delete a user
        String deletedMessage = username + " deleted successfully.";
        Request deleteRequest = new Request("DELETE", ENDPOINT + "/users/" + username);
        request.setOptions(options());
        response = client().performRequest(deleteRequest);
        assertEquals(response.getStatusLine().getStatusCode(), 200);
        Map<String, Object> deletedUsers = entityAsMap(response);
        assertEquals(deletedUsers.size(), 2);
        assertEquals(deletedUsers.get("successful"), true);
        assertEquals(deletedUsers.get("message"), deletedMessage);

    }

    public void testResetPassword() throws Exception {
        String username = "test-user-00";
        String userCreationContent = "{ \"password\" : \"test\"" + " }\n";

        String requestContent = "{ \"oldpassword\" : \"test\"," + " \"newpassword\": \"testnewpassword\"" + " }\n";

        String createMessage = username + " created successfully.";
        Request userCreationRequest = new Request("PUT", ENDPOINT + "/users/" + username);
        userCreationRequest.setJsonEntity(userCreationContent);
        userCreationRequest.setOptions(systemIndexWarning());
        Response userCreationResponse = client().performRequest(userCreationRequest);
        assertEquals(userCreationResponse.getStatusLine().getStatusCode(), 200);
        Map<String, Object> userCreated = entityAsMap(userCreationResponse);
        assertEquals(userCreated.size(), 3);
        assertEquals(userCreated.get("successful"), true);
        assertEquals(userCreated.get("username"), username);
        assertEquals(userCreated.get("message"), createMessage);

        Request request = new Request("POST", ENDPOINT + "/users/" + username + "/resetpassword");
        request.setJsonEntity(requestContent);
        request.setOptions(systemIndexWarning());
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    public void testResetPasswordWithNotExistedUser() throws Exception {
        String username = "test-user-not-existed";
        String requestContent = "{ \"oldpassword\" : \"test\"," + " \"newpassword\": \"testnewpassword\"" + " }\n";

        Request request = new Request("POST", ENDPOINT + "/users/" + username + "/resetpassword");
        request.setJsonEntity(requestContent);
        request.setOptions(systemIndexWarning());
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        Map<String, Object> exception = entityAsMap(e.getResponse());
        assertEquals(400, exception.get("status"));
        assertEquals(ErrorType.USER_NOT_EXISTING.getMessage(), ((Map<String, Object>) exception.get("error")).get("reason"));
    }

    public void testResetPasswordWithBadRequests() throws Exception {
        String username = "test-user-01";
        String userCreationContent = "{ \"password\" : \"test\"" + " }\n";

        String newPasswordsMatchOldPassword = "{ \"oldpassword\" : \"test\"," + " \"newpassword\": \"test\"" + " }\n";
        String oldPasswordsDontMatch = "{ \"oldpassword\" : \"wrongoldpassword\"," + " \"newpassword\": \"testnewpassword\"" + " }\n";

        String createMessage = username + " created successfully.";
        Request userCreationRequest = new Request("PUT", ENDPOINT + "/users/" + username);
        userCreationRequest.setJsonEntity(userCreationContent);
        userCreationRequest.setOptions(systemIndexWarning());
        Response userCreationResponse = client().performRequest(userCreationRequest);
        assertEquals(userCreationResponse.getStatusLine().getStatusCode(), 200);
        Map<String, Object> userCreated = entityAsMap(userCreationResponse);
        assertEquals(userCreated.size(), 3);
        assertEquals(userCreated.get("successful"), true);
        assertEquals(userCreated.get("username"), username);
        assertEquals(userCreated.get("message"), createMessage);

        // Old password mismatching
        Request requestOldPasswordMismatching = new Request("POST", ENDPOINT + "/users/" + username + "/resetpassword");
        requestOldPasswordMismatching.setJsonEntity(oldPasswordsDontMatch);
        requestOldPasswordMismatching.setOptions(systemIndexWarning());
        ResponseException eOldPasswordMismatching = expectThrows(
            ResponseException.class,
            () -> client().performRequest(requestOldPasswordMismatching)
        );
        Map<String, Object> exceptionOldPasswordMismatching = entityAsMap(eOldPasswordMismatching.getResponse());
        assertEquals(400, exceptionOldPasswordMismatching.get("status"));
        assertEquals(
            ErrorType.OLDPASSWORD_MISMATCHING.getMessage(),
            ((Map<String, Object>) exceptionOldPasswordMismatching.get("error")).get("reason")
        );

        // New password matching old password
        Request requestNewPasswordMatchingOldPassword = new Request("POST", ENDPOINT + "/users/" + username + "/resetpassword");
        requestNewPasswordMatchingOldPassword.setJsonEntity(newPasswordsMatchOldPassword);
        requestNewPasswordMatchingOldPassword.setOptions(systemIndexWarning());
        ResponseException eNewPasswordMatchingOldPassword = expectThrows(
            ResponseException.class,
            () -> client().performRequest(requestNewPasswordMatchingOldPassword)
        );
        Map<String, Object> exceptionNewPasswordMatchingOldPassword = entityAsMap(eNewPasswordMatchingOldPassword.getResponse());
        assertEquals(400, exceptionNewPasswordMatchingOldPassword.get("status"));
        assertEquals(
            ErrorType.NEWPASSWORD_MATCHING_OLDPASSWORD.getMessage(),
            ((Map<String, Object>) exceptionNewPasswordMatchingOldPassword.get("error")).get("reason")
        );
    }
}
