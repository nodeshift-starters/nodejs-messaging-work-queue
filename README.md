# Messaging Work Queue Example for Node.js

## Purpose

This example application demonstrates how to dispatch tasks to a scalable
set of worker processes using a message queue. It uses the AMQP 1.0
message protocol to send and receive messages.

## Prerequisites

* Node.js version 16

* The user has access to an OpenShift instance and is logged in.

* The user has selected a project in which the frontend and backend
  processes will be deployed.

## Deployment

Run the following commands to configure and deploy the applications.

```bash
$ oc new-project my-example-project
$ oc create -f service.amqp.yaml
$ ./start-openshift.sh
```
## Modules

The `frontend` module serves the web interface and communicates with
workers in the backend.

The `worker` module implements the worker service in the backend.

### OpenTelemetry with OpenShift Distributed Tracing Platform

This [link](./OTEL.md) contains instructions on how to install the 
OpenShift Distributed Tracing Platform and enable tracing. 