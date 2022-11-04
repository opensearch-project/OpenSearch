# Distributions
This subproject contains the necessary tooling to build the various distributions.
Note that some of this can only be run on the specific architecture and does not support cross-compile.

The following distributions are being built:
* Archives (`*.zip`, `*.tar`): these form the basis for all other OpenSearch distributions
* Packages (`*.deb`, `*.rpm`): specific package formats for some Linux distributions
* Docker images
* Backwards compatibility tests: used internally for version compatibility testing, not for public consumption

## With or Without JDK?
For each supported platform there should be both a target bundled with a JDK and a target without a bundled JDK.
