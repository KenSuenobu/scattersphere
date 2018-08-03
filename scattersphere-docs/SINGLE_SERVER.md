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
need to be (de)registered, at will, in real time on the server.  As a `Task`
is running that is part of a JAR file, that file should be locked in memory for the
job to run, and should unlock after all `Tasks` within that JAR file have
completed.

JAR files should automatically refresh registration if the same JAR file has changed
on the filesystem.  The process would be:

- Load in a registered JAR file
- Keep track of the registered JAR file in memory with a file watcher
- Search for all `Task` objects within the JAR file
- When a `Task` is requested to run, that `Task` is instantiated, and run in the JVM
- If the file changes (timestamp or filesize), the JAR file is unregistered, and the
  process repeats itself.
  
## Task auto-(re)registration

When a JAR file is registered or refreshed in Scattersphere, the following actions
occur:

- Registry of known `RunnableTask` objects is cleared
- `ClassLoader` for each of the registered `RunnableTask` objects is released
- The JAR file that was loaded is checked for any classes that extend `RunnableTask`, and are
  registered by their class names.

This way, any tasks that are started can be called by specifying the full classpath
to the `RunnableTask` object.  Only objects that extend `RunnableTask` may be
run dynamically.

## Instantiation of Tasks and Jobs

`Tasks` do not need to be instantiated through the system, however, `Job` objects do.
Since a `Job` is a series of `Tasks` with dependencies, these need to be
defined by hand.  Therefore, any `Job` objects that are instantiated need to keep track
of the `Tasks` that they rely on.  Once the associated `Job` finishes, the `Task` lock
is removed.

Once a `Job` is defined by on the server, the `Job` can be re-run and used as many times
as necessary by simply telling the server to start the job by the given name.

## Serializable Job Definitions

A `JobDefinition` object will need to be created to describe how a `Job` is constructed
from the given available `Tasks` that have been registered.

The `JobDefinition` object is loaded _after_ all of the associated `Tasks` have been
registered in the server.  Once the `JobDefinition` objects have been deserialized into
the server, they are automatically registered, and available for instantiation as the
user sees fit.

`JobDefinition` objects should only be saved on the filesystem on demand, they should
not be done by the server automatically (unless otherwise designed.)  This way, a
`JobDefinition` is not persisted without specifically telling the server to write the
data to disk.  `JobDefinitions` should be persisted using JSON, so that these entries
can be easily modified by hand as required.


