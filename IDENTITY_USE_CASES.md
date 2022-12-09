# Identity Use Cases in OpenSearch
- [Identity Use Cases in OpenSearch](#identity-use-cases-in-opensearch)
  - [Non-use compatibility](#non-use-compatibility)
    - [Scenario 1:](#scenario-1)
  - [Identity features enabled](#identity-features-enabled)
    - [Scenario 2:](#scenario-2)
    - [Scenario 3:](#scenario-3)
  - [Using Admin account](#using-admin-account)
    - [Scenario 4:](#scenario-4)
    - [Scenario 5:](#scenario-5)
  - [Password changes](#password-changes)
    - [Scenario 6:](#scenario-6)
    - [Scenario 7:](#scenario-7)
    - [Scenario 8:](#scenario-8)
  - [Authenticate as new user](#authenticate-as-new-user)
    - [Scenario 9:](#scenario-9)
    - [Scenario 10:](#scenario-10)
    - [Scenario 11:](#scenario-11)
  - [Multi-node Cluster](#multi-node-cluster)
    - [Scenario XXX:](#scenario-xxx)

## Non-use compatibility

    Given: Start an OpenSearch cluster with Identity experiment enabled and not configured

### Scenario 1:

The cluster runs with the Security plugin. No Security plugin scenarios are impacted.

*Technical*: Identity uses the NoopAuthenticationManager

## Identity features enabled

    Given: Start an OpenSearch cluster with Identity experiment enabled, internal identity is enabled in the configuration

### Scenario 2:

By reading the opensearch.log / console output a random admin password is shown.

### Scenario 3:

All REST API activity returns 403 without passing authentication information in the request

## Using Admin account

    Given: The admin password is known

### Scenario 4:

Using http basic auth it is possible to make any REST API request, no errors related to authentication are seen.

### Scenario 5:

Admin makes a request to `PUT /identity/user/admin/password` with a json body `{ password: "value" }`, it succeeds

*Technical*: Need user actions

## Password changes

    Given: The Admin password has been updated

### Scenario 6:

The admin's old password does not work on any REST API requests

### Scenario 7:

The admin's new password works for all REST API requests

### Scenario 8:

Admin user can create an account via `POST /identity/user/{username}`.  The response includes an automatically generated password for this user.

## Authenticate as new user

    Given: The User's user name and password are know

### Scenario 9:

The user account can be accessed with http basic auth.

### Scenario 10:

`GET /identity/whoami` returns the username of the authenticated account

### Scenario 11:

After the cluster is shutdown and restarted the accounts are still accessible as before

*Technical*: Need persistent storage


## Multi-node Cluster
    Given: The OpenSearch cluster has more than one node

### Scenario XXX:

Admin makes a request to `PUT {NODE_1}/identity/user/admin/password` with a json body `{ password: "value" }`, it succeeds.  Making a request to {NODE_2} requires the updated password, using the old password returns a 403 on REST API requests. 
