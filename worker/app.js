//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// 'License'); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

'use strict';

const path = require('path');
const crypto = require('crypto');
const express = require('express');
const probe = require('kube-probe');
const rhea = require('rhea');

const amqpHost = process.env.MESSAGING_SERVICE_HOST || 'localhost';
const amqpPort = process.env.MESSAGING_SERVICE_PORT || 5672;
const amqpUser = process.env.MESSAGING_SERVICE_USER || 'work-queue';
const amqpPassword = process.env.MESSAGING_SERVICE_PASSWORD || 'work-queue';

// AMQP

const id = `worker-nodejs-${crypto.randomBytes(2).toString('hex')}`;
const container = rhea.create_container({ id });

let workerUpdateSender = null;
let requestsProcessed = 0;
let processingErrors = 0;

function processRequest (request) {
  const uppercase = request.application_properties.uppercase;
  const reverse = request.application_properties.reverse;
  let text = request.body;

  if (uppercase) {
    text = text.toUpperCase();
  }

  if (reverse) {
    text = text.split('').reverse().join('');
  }

  return text;
}

container.on('connection_open', event => {
  console.log(`${id}: Connected to AMQP messaging service at ${amqpHost}:${amqpPort}`);

  event.connection.open_receiver('work-queue/requests');
  workerUpdateSender = event.connection.open_sender('work-queue/worker-updates');
});

container.on('message', event => {
  const request = event.message;
  let responseBody;

  console.log(`${id}: Received request ${request}`);

  try {
    responseBody = processRequest(request);
  } catch (e) {
    console.error(`${id}: Failed processing message: ${e}`);
    processingErrors++;
    return;
  }

  const response = {
    to: request.reply_to,
    correlation_id: request.message_id,
    application_properties: {
      workerId: container.id
    },
    body: responseBody
  };

  event.connection.send(response);

  requestsProcessed++;

  console.log(`${id}: Sent response ${JSON.stringify(response)}`);
});

function sendUpdate () {
  if (!workerUpdateSender || !workerUpdateSender.sendable()) {
    return;
  }

  const update = {
    application_properties: {
      workerId: container.id,
      timestamp: new Date().getTime(),
      requestsProcessed,
      processingErrors
    }
  };

  workerUpdateSender.send(update);
}

setInterval(sendUpdate, 5 * 1000);

const opts = {
  host: amqpHost,
  port: amqpPort,
  username: amqpUser,
  password: amqpPassword
};

console.log(`${id}: Attempting to connect to AMQP messaging service at ${amqpHost}:${amqpPort}`);
container.connect(opts);

// HTTP

const app = express();

// Expose the license.html at http[s]://[host]:[port]/licenses/licenses.html
app.use('/licenses', express.static(path.join(__dirname, 'licenses')));

probe(app);

module.exports = app;
