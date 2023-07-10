from httprequest_blueprints import execute_request
import argparse
import json
import sys
import requests.auth
from dateutil import parser
import pytz
import datetime
import shipyard_utils


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--api-key', dest='api_key', required=True)
    parser.add_argument('--api-secret', dest='api_secret', required=True)
    parser.add_argument('--connector-id', dest='connector_id', required=False)
    return parser.parse_args()


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
            pytz.utc) - datetime.timedelta(days=10000)

    if last_failure:
        last_failure = parser.parse(last_failure)
    else:
        last_failure = datetime.datetime.now(
            pytz.utc) - datetime.timedelta(days=10000)

    if execution_time:
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
    else:
        if last_failure > last_success:
            print(
                f'Fivetran reports that the connector {connector_id} recently errored at {last_failure}.')
            exit_code = 1
        else:
            print(
                f'Fivetran reports that connector {connector_id} was recently successful at {last_success}.')
            exit_code = 0

    return exit_code



def main():
    args = get_args()
    api_key = args.api_key
    api_secret = args.api_secret
    auth_header = requests.auth._basic_auth_str(api_key, api_secret)
    headers = {'Authorization': auth_header}
    execution_time = None

    base_folder_name = shipyard_utils.logs.determine_base_artifact_folder(
        'fivetran')
    artifact_subfolder_paths = shipyard_utils.logs.determine_artifact_subfolders(
        base_folder_name)
    shipyard_utils.logs.create_artifacts_folders(artifact_subfolder_paths)
    
    
    if args.connector_id and args.connector_id !='':
        connector_id = args.connector_id
    else:
        connector_id = shipyard_utils.logs.read_pickle_file(
                artifact_subfolder_paths, 'connector_id')
        execution_time= shipyard_utils.logs.read_pickle_file(
                artifact_subfolder_paths, 'execution_time')

    connector_details_response = get_connector_details(
        connector_id,
        headers,
        folder_name=f'{base_folder_name}/responses',
        file_name=f'connector_{connector_id}_response.json')

    if connector_details_response['code'] == 'Success':

        sys.exit(determine_sync_status(
            connector_details_response, execution_time))
    else:
        print(connector_details_response['message'])
        sys.exit(1)



if __name__ == '__main__':
    main()