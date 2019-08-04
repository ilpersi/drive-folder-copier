# standard imports
import csv
import pickle
import multiprocessing
from datetime import datetime
import logging.config
import logging.handlers
import os

from functools import partial

# import ad hoc developed modules

import drivecopyutils
from backoff import call_endpoint

# third parties import
import click
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

from googleapiclient.discovery import build

click.option = partial(click.option, show_default=True)


@click.command()
@click.option('--client-secret', '-cs', metavar='<client_secret_json_file>', default='client_id.json',
              help='Path to OAuth2 client secret JSON file.')
@click.option('--start-id', '-s', metavar='<start_email_address>', required=True,
              help='Email address of the source account')
@click.option('--target-id', '-t', metavar='<dest_email_address>', required=True,
              help='Email address of the target account')
@click.option('--workers-cnt', '-n', default=multiprocessing.cpu_count() * 2,
              help='The number of concurrent processes you want to run.')
@click.option('--folder-id', '-id', metavar='<folder_id>', default='root',
              help='The id of the folder you want to copy. My Drive will be used as default',)
@click.option('--mapping-report', '-mr', metavar='<mapping_csv>', show_default=True, default='file_mapping.csv',
              help='Path where to save the csv output file containing the copy mapping.')
@click.option('--max-size', '-mx', metavar='<bytes>', default=0,
              help='If file size in bytes is bigger that this value, the copy will be skipped. Set to 0 for no limits.')
def main(client_secret, start_id, target_id, workers_cnt, folder_id, mapping_report, max_size):
    """This tool will allow you to copy files and folders between domains without modifying the source sharing
    permissions."""
    dt_start = datetime.now()

    scopes = ['https://www.googleapis.com/auth/drive',
              'https://www.googleapis.com/auth/spreadsheets', ]

    mp_manager = multiprocessing.Manager()
    unsearched = multiprocessing.JoinableQueue()
    folder_mapping = mp_manager.dict()
    copy_mapping = mp_manager.list()
    logger_q = multiprocessing.Queue()

    logging_config_initial = {
        'version': 1,
        'formatters': {
            'detailed': {
                'class': 'logging.Formatter',
                'format': '%(asctime)s %(name)-15s %(levelname)-8s %(processName)-10s %(message)s'
            }
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'level': 'INFO',
            },
        },
        'root': {
            'level': 'DEBUG',
            'handlers': ['console']
        },
    }

    # START ID CREDENTIAL FLOW
    start_creds_file_name = os.path.join(click.get_app_dir('drive-folder-copier'),
                                         '{}-credentials.dat'.format(start_id))

    config_path = os.path.dirname(start_creds_file_name)
    if config_path and not os.path.isdir(config_path):
        os.makedirs(config_path)

    if not os.path.exists(start_creds_file_name):

        flow = InstalledAppFlow.from_client_secrets_file(client_secret, scopes)
        start_creds = flow.run_local_server()

        with open(start_creds_file_name, 'wb') as credentials_dat:
            pickle.dump(start_creds, credentials_dat)
    else:
        with open(start_creds_file_name, 'rb') as credentials_dat:
            start_creds = pickle.load(credentials_dat)

    if start_creds.expired:
        start_creds.refresh(Request())

    # DEST ID CREDENTIAL FLOW
    dest_creds_file_name = os.path.join(click.get_app_dir('drive-folder-copier'),
                                        '{}-credentials.dat'.format(target_id))

    config_path = os.path.dirname(dest_creds_file_name)
    if config_path and not os.path.isdir(config_path):
        os.makedirs(config_path)

    if not os.path.exists(dest_creds_file_name):

        flow = InstalledAppFlow.from_client_secrets_file('client_id.json', scopes)
        dest_creds = flow.run_local_server()

        with open(dest_creds_file_name, 'wb') as credentials_dat:
            pickle.dump(dest_creds, credentials_dat)
    else:
        with open(dest_creds_file_name, 'rb') as credentials_dat:
            dest_creds = pickle.load(credentials_dat)

    if dest_creds.expired:
        dest_creds.refresh(Request())

    start_drive_sdk = build('drive', 'v3', credentials=start_creds)
    dest_drive_sdk = build('drive', 'v3', credentials=dest_creds)

    drive_get_params = {
        'fileId': folder_id,
        'fields': 'id,name',
    }

    logging.config.dictConfig(logging_config_initial)
    logger = logging.getLogger('setup')
    logger.info('About to create workers ...')
    workers = [drivecopyutils.DriveWorker(start_creds_file_name, dest_creds_file_name, unsearched, folder_mapping,
                                          copy_mapping, logger_q, start_id, target_id, max_size, scopes)
               for _ in range(workers_cnt)]

    # we get the details of the root folder we want to copy
    root_folder_details = call_endpoint(start_drive_sdk.files().get, drive_get_params)

    # we create the root folder on the destination account
    drive_insert_params = {
        'body': {
            'name': '{} copied from {}'.format(root_folder_details.get('name'), start_id),
            'mimeType': 'application/vnd.google-apps.folder',
        },
        'fields': 'id',
    }
    root_copy = call_endpoint(dest_drive_sdk.files().create, drive_insert_params)
    folder_mapping[root_folder_details.get('id')] = root_copy.get('id')

    # we create a temporary folder on the start account
    drive_tmp_params = {
        'body': {
            'name': 'TMP copy of {} for {}'.format(root_folder_details.get('name'), target_id),
            'mimeType': 'application/vnd.google-apps.folder',
        },
        'fields': 'id',
    }
    start_tmp = call_endpoint(start_drive_sdk.files().create, drive_tmp_params)
    folder_mapping['root_copy_tmp'] = start_tmp.get('id')

    # we share the temporary folder with the destination account in edit
    start_share_params = {
        'sendNotificationEmail': False,
        'fileId': start_tmp.get('id'),
        'body': {
            'emailAddress': target_id,
            'type': 'user',
            'role': 'writer'
        },
        'fields': 'id',
    }
    call_endpoint(start_drive_sdk.permissions().create, start_share_params)

    unsearched.put(root_folder_details)

    logger.info('About to create listener ...')
    stop_event = multiprocessing.Event()
    listener = drivecopyutils.LoggingListener(logger_q, stop_event, start_id, target_id)
    listener.start()
    logger.info('Started listener')

    for worker in workers:
        worker.start()

    unsearched.join()

    for i in range(workers_cnt):
        unsearched.put(None)

    stop_event.set()
    listener.join()

    # we delete the temporary folder on the origin account
    tmp_del_params = {
        'fileId': start_tmp.get('id'),
    }
    call_endpoint(start_drive_sdk.files().delete, tmp_del_params)

    # we save the mappings in a dedicated csv file
    with open(mapping_report, 'w', newline='', encoding='utf8') as csv_file:
        write_header = True
        writer = None

        for mapping in copy_mapping:
            if write_header:
                writer = csv.DictWriter(csv_file, mapping.keys())
                writer.writeheader()
                write_header = False

            writer.writerow(mapping)

    print(datetime.now() - dt_start)


if __name__ == '__main__':
    multiprocessing.freeze_support()
    main()
