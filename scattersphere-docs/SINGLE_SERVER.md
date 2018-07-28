# Scattersphere Single Server

This document describes the single server use of Scattersphere as a service.  It
utilizes a registerable set of `.jar` files that contain `Task` objects, which
can be instantiated and run on the fly.

## Purpose

Scattersphere as an API is useful only when used in an application.  Scattersphere
as a server allows `.jar` file versions of `Task` objects to be registered and
de-registered, with the ability to run a `Task` from the server on demand.

