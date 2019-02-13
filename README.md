# Messaging Work Queue Mission for Node.js

[![Greenkeeper badge](https://badges.greenkeeper.io/nodeshift-starters/nodejs-messaging-work-queue.svg)](https://greenkeeper.io/)

## Purpose

This mission booster demonstrates how to dispatch tasks to a scalable
set of worker processes using a message queue. It uses the AMQP 1.0
message protocol to send and receive messages.

## Prerequisites

* The user has access to an OpenShift instance and is logged in.

* The user has selected a project in which the frontend and backend
  processes will be deployed.

## Deployment

Run the following commands to configure and deploy the applications.

```bash
$ oc create -f service.amqp.yaml

$ ./start-openshift
```
## Modules

The `frontend` module serves the web interface and communicates with
workers in the backend.

The `worker` module implements the worker service in the backend.
