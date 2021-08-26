from httprequest_blueprints import execute_request
import argparse
import os
import json
import sys
import pickle
import requests.auth
from dateutil import parser
import pytz
import datetime


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--api-key', dest='api_key', required=True)
    parser.add_argument('--api-secret', dest='api_secret', required=True)
    parser.add_argument('--connector-id', dest='connector_id', required=False)
    args = parser.parse_args()
    return args


def write_json_to_file(json_object, file_name):
    with open(file_name, 'w') as f:
        f.write(
            json.dumps(
                json_object,
                ensure_ascii=False,
                indent=4))
    print(f'Response stored at {file_name}')


def get_connector_details(
        connector_id,
        headers,
        folder_name,
        file_name=f'connector_details_response.json'):
    get_connector_details_url = f'https://api.fivetran.com/v1/connectors/{connector_id}/'
    print(f'Grabbing details for connector {connector_id}.')
    connector_details_req = execute_request.execute_request(
        'GET', get_connector_details_url, headers=headers)
    connector_details_response = json.loads(connector_details_req.text)
    execute_request.create_folder_if_dne(folder_name)
    combined_name = execute_request.combine_folder_and_file_name(
        folder_name, file_name)
    write_json_to_file(connector_details_response, combined_name)
    return connector_details_response


def determine_sync_status(connector_details_response, execution_time):
    connector_id = connector_details_response['data']['id']
    last_success = connector_details_response['data']['succeeded_at']
    last_failure = connector_details_response['data']['failed_at']

    # Handling for when results come back as null.
    if last_success:
        last_success = parser.parse(last_success)
    else:
        last_success = datetime.datetime.now(
            pytz.utc) - datetime.timedelta(days=1)

    if last_failure:
        last_failure = parser.parse(last_failure)
    else:
        last_failure = datetime.datetime.now(
            pytz.utc) - datetime.timedelta(days=1)

    if (last_success > execution_time) or (last_failure > execution_time):
        if last_failure > execution_time:
            print(
                f'Fivetran reports that the connector {connector_id} recently errored at {last_failure}.')
            exit_code = 1
        else:
            print(
                f'Fivetran reports that connector {connector_id} was recently successful at {last_success}.')
            exit_code = 0
    else:
        print(
            f'Fivetran reports that the connector {connector_id} has not yet completed since the last execution time of {execution_time}')
        exit_code = 255
    return exit_code


def working_pickle_file(pickle_folder_name, pickle_file_name):
    full_pickle_path = execute_request.combine_folder_and_file_name(
        pickle_folder_name, pickle_file_name)
    if os.path.exists(full_pickle_path):
        return full_pickle_path
    else:
        return None


def load_pickle_variables(full_pickle_path):
    with open(full_pickle_path, 'rb') as f:
        connector_id, execution_time = pickle.load(f)

    return connector_id, execution_time


def main():
    args = get_args()
    api_key = args.api_key
    api_secret = args.api_secret
    auth_header = requests.auth._basic_auth_str(api_key, api_secret)
    headers = {'Authorization': auth_header}

    shipyard_upstream_vessels = os.environ.get(
        "SHIPYARD_FLEET_UPSTREAM_LOG_IDS")

    artifact_directory_default = f'{os.environ.get("USER")}-artifacts'
    base_folder_name = execute_request.clean_folder_name(
        f'{os.environ.get("SHIPYARD_ARTIFACTS_DIRECTORY",artifact_directory_default)}/fivetran-blueprints/')

    pickle_folder_name = execute_request.clean_folder_name(
        f'{base_folder_name}/variables')
    execute_request.create_folder_if_dne(pickle_folder_name)

    connector_id = None
    execution_time = None
    if args.connector_id:
        connector_id = args.connector_id
        execution_time = datetime.datetime.now(pytz.utc)
    elif shipyard_upstream_vessels:
        shipyard_upstream_vessels = shipyard_upstream_vessels.split(',')
        for vessel_id in shipyard_upstream_vessels:
            full_pickle_path = working_pickle_file(
                pickle_folder_name,
                f'{vessel_id}_force_sync.pickle')
            if full_pickle_path:
                connector_id, execution_time = load_pickle_variables(
                    full_pickle_path)

    if not connector_id and not execution_time:
        full_pickle_path = working_pickle_file(
            pickle_folder_name,
            f'force_sync.pickle')
        if full_pickle_path:
            connector_id, execution_time = load_pickle_variables(
                full_pickle_path)

    connector_details_response = get_connector_details(
        connector_id,
        headers,
        folder_name=f'{base_folder_name}/responses',
        file_name=f'connector_{connector_id}_response.json')

    sys.exit(determine_sync_status(
        connector_details_response, execution_time))


if __name__ == '__main__':
    main()
