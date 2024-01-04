# Indexer development environments

Install [Docker Desktop][docker-desktop] as per its instructions, available for Windows, Mac
and Linux (Ubuntu, Debian & Fedora).
This ensures that the development experience between Linux, Mac and Windows is as
similar as possible.

> IMPORTANT: be methodic during the installation of Docker Desktop, and proceed
> step by step as described in their documentation. Make sure that your system
> meets the system requirements before installing Docker Desktop, and read any
> post-installation note, specially on Linux: [Differences between
> Docker Desktop for Linux and Docker Engine][docker-variant].

## Pre-requisites

1. Assign resources to [Docker Desktop][docker-desktop]. The requirements for the
   environments are:

   - 8 GB of RAM (minimum)
   - 4 cores

   The more resources the better ☺

2. Clone the [wazuh-indexer][wi-repo].

3. Set up user permissions

   The Docker volumes will be created by the internal Docker user, making them
   read-only. To prevent this, a new group named `docker-desktop` and GUID 100999
   needs to be created, then added to your user and the source code folder:

   ```bash
   sudo groupadd -g 100999 docker-desktop
   sudo useradd -u 100999 -g 100999 -M docker-desktop
   sudo chown -R docker-desktop:docker-desktop $WZD_HOME
   sudo usermod -aG docker-desktop $USER
   ```

## Understanding Docker contexts

Before we begin starting Docker containers, we need to understand the
differences between Docker Engine and Docker Desktop, more precisely, that the
use different contexts.

Carefully read these two sections of the Docker documentation:

- [Differences between Docker Desktop for Linux and Docker Engine][docker-variant].
- [Switch between Docker Desktop and Docker Engine][docker-context].

Docker Desktop will change to its context automatically at start, so be sure
that any existing Docker container using the default context is **stopped**
before starting Docker Desktop and any of the environments in this folder.

## Starting up the environments

Use the sh script to up the environment.

Example:

```bash
Usage: ./dev.sh {up|down|stop} [security]
```

Once the `wazuh-indexer` container is up, attach a shell to it and run `./gradlew run`
to start the application.


[docker-desktop]: https://docs.docker.com/get-docker
[docker-variant]: https://docs.docker.com/desktop/install/linux-install/#differences-between-docker-desktop-for-linux-and-docker-engine
[docker-context]: https://docs.docker.com/desktop/install/linux-install/#context
[wi-repo]: https://github.com/wazuh/wazuh-indexer
