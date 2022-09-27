# Identity Roadmap - Native AuthN/AuthZ

Every thing done within OpenSearch should be permissible, no matter the source.  This is why we need to move authentication and authorization into OpenSearch core codebase.  The plugin model is very useful for optional things, security primitives are not one of them.

Getting these new native concepts out to OpenSearch developers and customers quickly for feedback and iteration is our aim to achieve this.

## Minimal viable system

To move quickly, and provide a surface area for experimentation we are going to cut some corners.  This effort will first be within a feature branch, then be controlled via an experimental feature flag shipped with OpenSearch distribution, finally made available for adoption with backwards compatible support.

The following are the areas of focus to create this minimal set of functionality along with an estimated complexity to develop to help in terms of understanding the projects.  Complexity attempts to score uncertainty, not effort, delivering more complex items implies more prototypes/design reviews.

### Identity Concepts

[Complexity 8]
Currently in Progress

### Delegate Execution

[Complexity 5]

### Access Tokens

- [Complexity 5] APIs, generate token for principal, refresh token, expire all refresh tokens
- [Complexity 5] Secrets generation / storage

### Internal Identity Provider

- [Complexity 2] In memory 
- [Complexity 3] Persistently stored
 
### Creation of Subject

[Complexity 3]

### Authorization

- [Complexity 2] Registration of permissions
- [Complexity 5] Registration of permissions for extension/plugins
- [Complexity 2] Subject -> List of permissions
- [Complexity 3] New permissions model
- [Complexity 8] Permission name schema
- [Complexity 2] Rename core permissions
- [Complexity 8] Rename plugin permissions (OOB?)
- [Complexity 2] Where does evaluation happen for FGAC?
- [Complexity 2] Grant permissions to a subject

### Http Basic Auth

[Complexity 3]

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