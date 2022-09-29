# Identity Roadmap - Native AuthN/AuthZ

Every thing done within OpenSearch should be permissible, no matter the source.  This is why we need to move authentication and authorization into OpenSearch core codebase.  The plugin model is very useful for optional things, security primitives are not one of them.

Getting these new native concepts out to OpenSearch developers and customers quickly for feedback and iteration is our aim to achieve this.

## Minimal viable system

To move quickly, and provide a surface area for experimentation we are going to cut some corners.  This effort will first be within a feature branch, then be controlled via an experimental feature flag shipped with OpenSearch distribution, finally made available for adoption with backwards compatible support.

The following are the areas of focus to create this minimal set of functionality along with an estimated complexity to develop to help in terms of understanding the projects.  Complexity attempts to score uncertainty, not effort, delivering more complex items implies more prototypes/design reviews.

### Identity Concepts

[Complexity 8] Flesh out the objects and methods associated with identity primitives such as Subject, Principal, tracking authorization state, communicating concepts into un-trusted ecosystems.  For additional details see

- https://github.com/opensearch-project/OpenSearch/issues/3846
- https://github.com/opensearch-project/opensearch-sdk-java/issues/37
- https://github.com/opensearch-project/opensearch-sdk-java/issues/40

### Delegate Execution

[Complexity 5] Allows transitioning into another Subject.  This should be possible with system subjects or interactive user sessions, the default mechanism should use access tokens that are cryptographically secured against tampering.

### Access Tokens

- [Complexity 5] APIs, generate token for principal, refresh token, expire all refresh tokens
- [Complexity 5] Secrets generation / storage

### Internal Identity Provider

- [Complexity 2] Create basis in memory user store for username/password based authentication 
- [Complexity 3] Persistently store the users across the cluster in a way that is keep consistent
 
### Creation of Subject

[Complexity 3] Allow creation of subjects for system processes or by users to run on their behalf

### Authorization

- [Complexity 2] Registration of permissions from all permissions sources in core
- [Complexity 5] Registration of permissions for extension/plugins
- [Complexity 2] Have a process where from a Subject a list of permissions is generated so they can be checked
- [Complexity 3] Design the permissions model and any potential changes.  Notability existing permissions based on action names will be on deprecation path 
- [Complexity 8] For the new permission come up with a structured naming convention building blocks, such as {SourceName}{ResourceName}{ActionVerb} -> Ad.Detector.Create.  Additionally come up with a 'blessed' list of action verbs to prevent http methods from being used as action names.
- [Complexity 2] With the new naming convention create updated names for all core permissions
- [Complexity 2] Confirm evaluation of permissions for Actions happen in
- [Complexity 2] Grant permissions to a subject, through the identity provider

### Http Basic Auth

[Complexity 3]
Support basic username/password authentication

## Out of Band

The following items are not required for the critical path delivery and will likely be started on following the stabilization of the MVP components.

### Loading from configuration

[Complexity 8]
Reuse/replace existing YAML configuration

### Index View(s)

[Complexity 8]
Might be further expanded into a generic 'resource collection'

### Authorization Abstractions

[Complexity 8]
Roles, groups, trees, or other intermediate representations of permissions.

### Rename plugin permissions (OOB?)

[Complexity 8]
Drive the renaming of permissions through all the existing plugins.

### Migration from Security Plugin

[Complexity 13]
How to do a complete, or partial migration(s) from the security plugin.

### Authentication providers

[Complexity 8]
OIDC, SAML, and other(s).

### Security testing

[Complexity 8]
How to author test cases for security features, and building out these test cases for all critical scenarios

### Identity Information

[Complexity 8]
Allow retrieve/storage of user information
