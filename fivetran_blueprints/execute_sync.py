from httprequest_blueprints import execute_request, download_file

import argparse
import os
import json
import time
import pickle
import sys
import requests.auth
import datetime
import pytz

try:
    import check_sync_status
except BaseException:
    from . import check_sync_status


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--api-key', dest='api_key', required=True)
    parser.add_argument('--api-secret', dest='api_secret', required=True)
    parser.add_argument('--connector-id', dest='connector_id', required=True)
    parser.add_argument(
        '--check-status',
        dest='check_status',
        default='TRUE',
        choices={
            'TRUE',
            'FALSE'},
        required=False)
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


def execute_sync(
        connector_id,
        headers,
        folder_name,
        file_name='sync_details_response.json'):
    execute_sync_url = f'https://api.fivetran.com/v1/connectors/{connector_id}/force'

    print(f'Starting to sync connector {connector_id}')
    sync_connector_req = execute_request.execute_request(
        'POST', execute_sync_url, headers)
    execution_time = datetime.datetime.now(pytz.utc)
    sync_connector_response = json.loads(sync_connector_req.text)
    execute_request.create_folder_if_dne(folder_name)
    combined_name = execute_request.combine_folder_and_file_name(
        folder_name, file_name)
    write_json_to_file(
        sync_connector_response, combined_name)
    return sync_connector_response, execution_time


def main():
    args = get_args()
    connector_id = args.connector_id
    api_key = args.api_key
    api_secret = args.api_secret
    check_status = execute_request.convert_to_boolean(args.check_status)
    auth_header = requests.auth._basic_auth_str(api_key, api_secret)
    headers = {'Authorization': auth_header}
    shipyard_log_id = os.environ.get("SHIPYARD_LOG_ID")

    artifact_directory_default = f'{os.environ.get("USER")}-artifacts'
    base_folder_name = execute_request.clean_folder_name(
        f'{os.environ.get("SHIPYARD_ARTIFACTS_DIRECTORY",artifact_directory_default)}/fivetran-blueprints/')

    sync_connector_response, execution_time = execute_sync(
        connector_id,
        headers,
        folder_name=f'{base_folder_name}/responses',
        file_name=f'snyc_{connector_id}_response.json')

    pickle_folder_name = execute_request.clean_folder_name(
        f'{base_folder_name}/variables')
    execute_request.create_folder_if_dne(pickle_folder_name)
    pickle_file_name = execute_request.combine_folder_and_file_name(
        pickle_folder_name,
        f'{shipyard_log_id + "_" if shipyard_log_id else ""}force_sync.pickle')
    with open(pickle_file_name, 'wb') as f:
        pickle.dump([connector_id, execution_time], f)

    sync_status = sync_connector_response['code']
    if sync_status == 'Success':
        if check_status:
            is_complete = False
            while not is_complete:
                connector_details_response = check_sync_status.get_connector_details(
                    connector_id,
                    headers,
                    folder_name=f'{base_folder_name}/responses',
                    file_name=f'connector_{connector_id}_response.json')
                exit_code = check_sync_status.determine_sync_status(
                    connector_details_response, execution_time)
                if exit_code == 255:
                    print(
                        f'{connector_id} has not finished syncing recently. Waiting 30 seconds and trying again.')
                    time.sleep(30)
                else:
                    is_complete = True
            sys.exit(exit_code)
        else:
            print(f'{sync_connector_response["message"]}')
            sys.exit(0)
    else:
        print(f'{sync_connector_response["message"]}')
        sys.exit(1)


if __name__ == '__main__':
    main()
