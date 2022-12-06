<img src="https://opensearch.org/assets/img/opensearch-logo-themed.svg" height="64px">

- [Access Tokens](#contributing-to-opensearch)
    - [How are Access Tokens used in OpenSearch](#how-are-access-tokens-used-in-opensearch)
    - [What Types of Authentication are Supported](#what-types-of-authentication-are-supported)



# Access Tokens

OpenSearch makes use of access tokens to manage account access after authentication and authorization are successful. Access tokens are short-lived tokens which grant the associated subject permissions in the form of access to a given domain API. You make use of access tokens during most day-to-day web operations. For example, many people are familiar with the process of logging-into a website using a username and password. You may have noticed that you can often leave the website page entirely, only to return to site later and find yourself able to use your account without having to login again. This persistence of your credentials is a common application of Access Tokens and one that OpenSearch leverages to provide a secure and intuitive user experience when running clusters.

## How are Access Tokens used in OpenSearch?

As mentioned, Access Tokens are a way for OpenSearch to manage the continued authentication of users. By providing short-term access, the tokens allow the user to execute calls to OpenSearch extensions without the immediate need to re-verify oneself. An Access Token wil last up to 1 hour before it must be refreshed. During that time, the user is treated as already authenticated by all extensions and OpenSearch processes.

Access Tokens are also used by OpenSearch in order to provide alternative methods of login through third-party Identity Providers (IdPs).

## What Types of Authentication are Supported?

Using Access Tokens, OpenSearch is able to support several types of authentication. Basic Authentication is supported through the use of username-password verification which then provides the subject an Access Token when login is successful. Bearer Authentication is supported through the use of [JWTs](./jwt.md) which provide an Access Token on the successful verification of a signed JWT.



