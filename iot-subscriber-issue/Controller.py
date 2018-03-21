# Copyright 2018 data-centric solutions ltd. All rights reserved.
# Based on the Google Inc. IoT end-to-end sample
# (https://cloud.google.com/iot/docs/samples/end-to-end-sample).
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Assumes the following already exist in Google Cloud:

Project                             : xyz-tests
Registry                            : simple-mqtt-registry
Topic                               : simple-mqtt-events
Publish Telemetry Subscription      : simple-mqtt-subscription

# Create a separate subscription for the device controller to read from
gcloud pubsub subscriptions create projects/xyz-tests/subscriptions/simple-mqtt-controller \
    --topic=projects/xyz-tests/topics/simple-mqtt-events

# Create and permission a service account for the controller to use
gcloud iam service-accounts create simple-mqtt-controller --display-name simple-mqtt-controller

gcloud projects add-iam-policy-binding xyz-tests \
    --member 'serviceAccount:simple-mqtt-controller@xyz-tests.iam.gserviceaccount.com' \
    --role 'roles/owner'

gcloud iam service-accounts keys create service-account.json \
  --iam-account simple-mqtt-controller@xyz-tests.iam.gserviceaccount.com

# Change to the python project we want to run
$ cd ~/_dev/datacentricity-public-samples/iot-subscriber-issue

# Create a virtual environment for the project
# (Needs to be Python3 otherwise we get an HTTP 403 error)
$ virtualenv --python python3 venv

# Activate the virtual environment
$ source venv/bin/activate

# Install required packages
$ pip install -r requirements.txt

python Controller.py \
    --project_id=xyz-tests \
    --pubsub_subscription=simple-mqtt-controller \
    --service_account_json=/Users/Fred/_dev/gcp-credentials/simple-mqtt-controller-service-account.json

# To de-activate the virtual environment and go back to global python:
$ deactivate
"""
import argparse
import base64
import json
import sys
from threading import Lock
import time

from google.cloud import pubsub
import google.cloud.pubsub_v1.subscriber.message as Message
from googleapiclient import discovery
from googleapiclient.errors import HttpError
from oauth2client.service_account import ServiceAccountCredentials
from google.api_core.exceptions import ServiceUnavailable

# Required for WorkaroundPolicy class
from google.cloud.pubsub_v1.subscriber.policy import thread
import grpc
import os

import logging
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"), format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger('Controller')

API_SCOPES = ['https://www.googleapis.com/auth/cloud-platform']
API_VERSION = 'v1'
DISCOVERY_API = 'https://cloudiot.googleapis.com/$discovery/rest'
SERVICE_NAME = 'cloudiot'
MAX_SVC_UNAVAILABLE_COUNT = 60


class WorkaroundPolicy(thread.Policy):

    def on_exception(self, exception):
        # If we are seeing UNAVALABLE then we need to retry (so return None)
        if isinstance(exception, ServiceUnavailable):
            logger.warning('Ignoring google.api_core.exceptions.ServiceUnavailable exception: {}'.format(exception))
            return
        elif getattr(exception, 'code', lambda: None)() == grpc.StatusCode.UNAVAILABLE:
            logger.warning('Ignoring grpc.StatusCode.UNAVAILABLE (Original exception: {})'.format(exception))
            return
        else:
            # For anything else fall back on the parent class behaviour
            super(WorkaroundPolicy, self).on_exception(exception)


class Controller(object):
    """Represents the state of the server"""
    service_unavailable_error_count = 0

    def __init__(self, json_service_account):
        logger.info('Creating credentials from JSON Key File: "{}"...'.format(json_service_account))
        credentials = ServiceAccountCredentials.from_json_keyfile_name(json_service_account, API_SCOPES)
        self.keep_alive = True
        self.restart = False
        # Define these at instance level to make it easier to re-start on StatusCode.UNAVAILABLE [code=8a75] error
        self.subscriber = None
        self.subscription = None

        if not credentials:
            msg = 'Could not load service account credential from {}'.format(credentials)
            logger.error(msg)
            sys.exit(msg)

        discovery_url = '{}?version={}'.format(DISCOVERY_API, API_VERSION)

        logger.info('Creating service from discovery URL: "{}"...'.format(discovery_url))
        self._service = discovery.build(
            SERVICE_NAME,
            API_VERSION,
            discoveryServiceUrl = discovery_url,
            credentials = credentials,
            cache_discovery = False
        )
        self._update_config_mutex = Lock()

    def _update_device_config(self, project_id, region, registry_id, device_id, data):
        """Pushes the data to the given device as configuration."""
        current_state = data['slow_down']
        config_data = None

        if data['speed'] <= 60:
            config_data = {'slow_down':False}
        elif data['speed'] > 150:
            config_data = {'slow_down':True}
        else:
            return

        # Only push config that has actually changed
        if (config_data != current_state):
            self._push_config(config_data, device_id, project_id, region, registry_id)

    def _push_config(self, config_data, device_id, project_id, region, registry_id):
        try:
            config_data_json = json.dumps(config_data)
            body = {
                'version_to_update': 0,
                'binary_data': base64.b64encode(config_data_json.encode('utf-8')).decode('ascii')
            }

            device_name = ('projects/{}/locations/{}/registries/{}/devices/{}'
                           .format(project_id, region, registry_id, device_id))

            request = self._service.projects().locations().registries().devices() \
                .modifyCloudToDeviceConfig(name = device_name, body = body)

            self._update_config_mutex.acquire()

            try:
                request.execute()
                logger.debug('Successfully executed request for device name: "{}"...'.format(device_name))
            except HttpError as e:
                # If the server responds with a HtppError, log it here, but
                # continue so that the message does not stay NACK'ed on the
                # pubsub channel.
                logger.warning('Error executing ModifyCloudToDeviceConfig: {}'.format(e))
            finally:
                self._update_config_mutex.release()
        except Exception as ex:
            logger.error('Pushing config to device threw an Exception: {}.'.format(ex))
            raise

    def run(self, project_name, subscription_name):
        """The main loop. Consumes messages from the Pub/Sub subscription."""

        try:
            logger.info('Creating subscriber for project: "{}" and subscription: "{}"...'
                        .format(project_name, subscription_name))

            # Specify a workaround policy to handle StatusCode.UNAVAILABLE [code=8a75] error (but may get CPU issues)
            subscriber = pubsub.SubscriberClient(policy_class = WorkaroundPolicy)

            # Alternatively, instantiate subscriber without the workaround to see full exception stack
            # subscriber = pubsub.SubscriberClient()

            subscription_path = subscriber.subscription_path(project_name, subscription_name)

            logger.info('Listening for messages on {}...'.format(subscription_path))
            subscription = subscriber.subscribe(subscription_path, self._callback)

            subscription.future.result()

            while True:
                time.sleep(60)
        # Despite the fact that WorkaroundPolicy.on_exception() seems to be able to distinguish
        # this type of exception, it is not being picked up in this catch block
        except ServiceUnavailable as ex:
            self.service_unavailable_error_count += 1
            if (self.service_unavailable_error_count > MAX_SVC_UNAVAILABLE_COUNT):
                logger.warning(
                    'Service was unavailable {} times out of a maximum of {} retry attempts, last exception: {'
                        .format(MAX_SVC_UNAVAILABLE_COUNT, self.service_unavailable_error_count, ex))
                raise
            else:
                logger.warning(
                    'Service may be temporarily unavailable (Retry: {}), ignoring exception: {}.'
                    .format(self.service_unavailable_error_count, ex))
        except Exception as ex:
            logger.error('Encountered unexpected Exception in Controller.run(): "{}".'.format(ex))

    def _callback(self, msg: Message):
        """Logic executed when a message is received from subscribed topic."""
        try:
            data = json.loads(msg.data)
        except ValueError as e:
            logger.error('Loading Payload ({}) threw an Exception: {}.'.format(msg.data, e))
            # For the prototype, if we can't read it, then discard it
            msg.ack()
            return

        device_project_id = msg.attributes['projectId']
        device_registry_id = msg.attributes['deviceRegistryId']
        device_id = msg.attributes['deviceId']
        device_region = msg.attributes['deviceRegistryLocation']

        # Send the config to the device.
        try:
            self._update_device_config(
              device_project_id,
              device_region,
              device_registry_id,
              device_id,
              data)

            # Acknowledge the consumed message. This will ensure that they are not redelivered to this subscription.
            msg.ack()
        except Exception as ex:
            logger.error('Encountered unexpected Exception in Controller._callback(): "{}").'.format(ex,))


def parse_command_line_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Example of Google Cloud IoT registry and device management.')
    # Required arguments
    parser.add_argument(
        '--project_id',
        default=os.environ.get("GOOGLE_CLOUD_PROJECT"),
        required=True,
        help='GCP cloud project name.')
    parser.add_argument(
        '--pubsub_subscription',
        required=True,
        help='Google Cloud Pub/Sub subscription name.')

    # Optional arguments
    parser.add_argument(
        '--service_account_json',
        default=os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"),
        help='Path to service account json file.')

    return parser.parse_args()

def main():
    args = parse_command_line_args()

    server = None
    try:
        server = Controller(args.service_account_json)
    except Exception as ex:
        logger.error('Controller() encountered unexpected Exception: {}.'.format(ex))

    try:
        server.run(args.project_id, args.pubsub_subscription)
    except Exception as ex:
        logger.error('server.run() encountered unexpected Exception: {}.'.format(ex))

if __name__ == '__main__':
    main()
