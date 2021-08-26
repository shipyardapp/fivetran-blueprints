from httprequest_blueprints import execute_request, download_file

import argparse
import os
import json
import sys
import requests.auth


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--username', dest='username', required=True)
    parser.add_argument('--password', dest='password', required=True)
    parser.add_argument('--connector-id', dest='connector_id', required=True)
    parser.add_argument(
        '--schedule-type',
        dest='schedule_type',
        default='',
        choices={
            '',
            'manual',
            'auto'},
        required=False)
    parser.add_argument(
        '--paused',
        dest='paused',
        default='',
        choices={
            '',
            'TRUE',
            'FALSE'},
        required=False)
    parser.add_argument(
        '--historical-sync',
        dest='historical_sync',
        default='',
        choices={
            '',
            'TRUE'},
        required=False)

    # All possible options are available here
    # https://fivetran.com/docs/rest-api/connectors#modifyaconnector

    # Documentation for historical sync appears to be wrong. Sending any value
    # sets it to True.

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


def update_connector(
        connector_id,
        headers,
        folder_name,
        message,
        file_name='connector_details_response.json'):
    update_connector_url = f'https://api.fivetran.com/v1/connectors/{connector_id}'

    print(f'Starting to update {connector_id}')
    job_run_req = execute_request.execute_request(
        'PATCH', update_connector_url, headers, message)
    update_response = json.loads(job_run_req.text)
    execute_request.create_folder_if_dne(folder_name)
    combined_name = execute_request.combine_folder_and_file_name(
        folder_name, file_name)
    write_json_to_file(
        update_response, combined_name)
    return update_response


def main():
    args = get_args()
    connector_id = args.connector_id
    username = args.username
    password = args.password

    auth_header = requests.auth._basic_auth_str(username, password)
    headers = {'Authorization': auth_header,
               'Content-Type': 'application/json'}

    fields_to_update = {}
    if args.schedule_type:
        schedule_type = args.schedule_type
        fields_to_update['schedule_type'] = schedule_type

    if args.paused:
        paused = execute_request.convert_to_boolean(args.paused)
        fields_to_update['paused'] = paused

    if args.historical_sync:
        historical_sync = execute_request.convert_to_boolean(
            args.historical_sync)
        fields_to_update['is_historical_sync'] = historical_sync

    message = json.dumps(fields_to_update)

    artifact_directory_default = f'{os.environ.get("USER")}-artifacts'
    base_folder_name = execute_request.clean_folder_name(
        f'{os.environ.get("SHIPYARD_ARTIFACTS_DIRECTORY",artifact_directory_default)}/fivetran-blueprints/')

    update_response = update_connector(
        connector_id,
        headers,
        folder_name=f'{base_folder_name}/responses',
        file_name=f'update_{connector_id}_response.json',
        message=message)

    sync_status = update_response['code']
    if sync_status == 'Success':
        print(
            f'Connector {connector_id} has been successfully updated.')
        print(f'The following changes were made:')
        print(fields_to_update)
        sys.exit(0)
    else:
        print(f'{update_response["message"]}')
        sys.exit(1)


if __name__ == '__main__':
    main()
