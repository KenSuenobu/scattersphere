# Scattersphere Single Server

This document describes the single server use of Scattersphere as a service.  It
utilizes a registerable set of `.jar` files that contain `Task` objects, which
can be instantiated and run on the fly.

## Purpose

Scattersphere as an API is useful only when used in an application.  Scattersphere
as a server allows `.jar` file versions of `Task` objects to be registered and
de-registered, with the ability to run a `Task` from the server on demand.

**Running Scattersphere as a server introduces new challenges:**

First, the server can only register `Task` objects, and maybe a few `Job` objects
to run some `Jobs`.  However, given the nature of a server, these `Job` definitions
should be made dynamic.  This means, a `Job` can be defined on the fly in the Server
environment.

Second, since the environment now runs as a server, there needs to be some way to
communicate with the server by connecting to it using a session.  This introduces
network functionality: Netty, VertX, or another solution needs to be decided on for
the server to accept remote connections.

Third, network security must be in place to prevent malicious attacks.  These could
be done through web service calls (ie. Dropwizard), or through a JDBC-like interface
to allow for `Jobs` and possibly `Tasks` to be defined on the fly.

# Technical details

## JAR file loading and registration

In order to keep the server up and running for the longest amount of time, JAR files
need to be (de)registered, at will, in real time on the server.  As a `Task` or `Job`
is running that is part of a JAR file, that file should be locked in memory for the
job to run, and should unlock after all `Tasks` or `Jobs` within that JAR file have
completed.

JAR files should automatically refresh registration if the same JAR file has changed
on the filesystem.  The process would be:

- Load in a registered JAR file
- Keep track of the registered JAR file in memory with a file watcher
- Search for all `Task` and `Job` objects within the JAR file
- When a `Task` is requested to run, that `Task` is instantiated, and run in the JVM
- If the file changes (timestamp or filesize), the JAR file is unregistered, and the
  process repeats itself.
  

