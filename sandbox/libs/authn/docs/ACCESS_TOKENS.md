<img src="https://opensearch.org/assets/img/opensearch-logo-themed.svg" height="64px">

- [Access Tokens](#contributing-to-opensearch)
    - [How are Access Tokens used in OpenSearch](#how-are-access-tokens-used-in-opensearch)
    - [What Types of Authentication are Supported](#what-types-of-authentication-are-supported)



# Access Tokens

OpenSearch makes use of access tokens to manage account access after authentication and authorization are successful. Access tokens are short-lived tokens which grant the associated subject permissions in the form of access to a given domain API. You make use of access tokens during most day-to-day web operations. For example, many people are familiar with the process of logging-into a website using a username and password. You may have noticed that you can often leave the website page entirely, only to return to the website later and find yourself able to use your account without having to login again. This persistence of your credentials is a common application of Access Tokens and one that OpenSearch leverages to provide a secure and intuitive user experience when running clusters.

## How are Access Tokens used in OpenSearch?

As mentioned, Access Tokens are a way for OpenSearch to manage the continued authentication of users. By providing short-term access, the tokens allow the user to execute calls to OpenSearch extensions without the immediate need to re-verify themselves. An Access Token wil last up to 1 hour before it must be refreshed. During that time, the user is treated as already authenticated by all extensions and OpenSearch processes.

Access Tokens are also used by OpenSearch in order to provide alternative methods of login through third-party Identity Providers (IdPs). If a user chooses to use a third-party IdP to login into OpenSearch via a directory hosted on another platform, an Access Token will be issued by the IdP to allow the user to access the IdPs own API. For more information on Access Tokens please visit the Okta AuthO [website](https://auth0.com/docs/secure/tokens/access-tokens).

## What Types of Authentication are Supported?

Using Access Tokens, OpenSearch is able to support several types of authentication. Basic Authentication is supported through the use of username-password verification which then provides the subject an Access Token when login is successful. Bearer Authentication is supported through the use of [JWTs](./jwt.md) which provide an Access Token on the successful verification of a signed JWT.

## What are some typical OpenSearch use cases?

System: OpenSearch and computer hardware to run it

Actors: User using OpenSearch

Stakeholders: Company using OpenSearch & OpenSearch maintainers

Preconditions: Successfully downloaded and configured OpenSearch with Access Token logic enabled

Trigger: The user has configured an OpenSearch cluster and wants to log back in; the user wants to access OpenSearch using another IdP

1. Access an OpenSearch cluster using Basic Authentication

    * Description: User accesses an existing OpenSearch cluster using Basic Authentication. It begins when a user first navigates to the login page of the OpenSearch dashboard and ends when they are successfully authenticated.
    * Actors: User
    * Trigger: The user wants to view their cluster information or operate on their cluster.
    * Preconditions: Actor has configured an OpenSearch cluster and created an account using the internal IdP.
    * Normal Flow:
      - Step 1: The user selects the
      - Step 2:
      - Step 3:
      - Step 4:

2. Access an OpenSearch cluster using Bearer Authentication from an External IdP

    * Description: User accesses an existing OpenSearch cluster using Bearer Authentication by redirect from an external identity provider.
    * Actors: User
    * Trigger: The user wants to view their cluster information or operate on their cluster.
    * Preconditions: Actor has configured an OpenSearch cluster and has an account configured with an external IdP which is connected to the OpenSearch instances.
    * Normal Flow:

