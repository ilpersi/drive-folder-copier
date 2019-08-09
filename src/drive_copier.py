# standard imports
import csv
import json
import pickle
import multiprocessing
import logging.config
import logging.handlers
import os

from datetime import datetime
from functools import partial

# import ad hoc developed modules

import drivecopyutils
from backoff import call_endpoint
from persistency import ResumeCopySQL

# third parties import
import click
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

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
              help='The id of the folder you want to copy. My Drive will be used as default', )
@click.option('--mapping-report', '-mr', metavar='<mapping_csv>', show_default=True, default='file_mapping.csv',
              help='Path where to save the csv output file containing the copy mapping.')
@click.option('--max-size', '-ms', metavar='<bytes>', default=0, type=int,
              help='If file size in bytes is bigger that this value, the copy will be skipped. Set to 0 for no limits.')
def main(client_secret, start_id, target_id, workers_cnt, folder_id, mapping_report, max_size):
    """This tool will allow you to copy files and folders between domains without modifying the source sharing
    permissions."""
    dt_start = datetime.now()

    scopes = ['https://www.googleapis.com/auth/drive',
              'https://www.googleapis.com/auth/spreadsheets', ]

    mp_manager = multiprocessing.Manager()

    folder_mapping = mp_manager.dict()
    copy_mapping = mp_manager.list()
    already_copied_data = mp_manager.dict()
    db_lock = mp_manager.Lock()

    unsearched = multiprocessing.JoinableQueue()
    logger_q = multiprocessing.Queue()

    logging_config_initial = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'detailed': {
                'class': 'logging.Formatter',
                'format': '%(asctime)s %(name)15s %(levelname)8s %(processName)-10s %(message)s'
            }
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'level': 'INFO',
                'formatter': 'detailed',
            },
        },
        'loggers': {
            'drive_copier': {
                'level': 'DEBUG',
                'handlers': ['console'],
                'propagate': False,
            },
        }
    }

    app_dir = click.get_app_dir('drive-folder-copier')

    # START ID CREDENTIAL FLOW
    start_creds_file_name = os.path.join(app_dir, '{}-credentials.dat'.format(start_id))

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
    dest_creds_file_name = os.path.join(app_dir, '{}-credentials.dat'.format(target_id))

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

    # we restore saved data
    db_persistency = ResumeCopySQL(start_id, target_id, folder_id, app_dir)

    for saved_start_id, saved_dest_id, saved_modifiedTime, saved_parents in db_persistency.resume():
        already_copied_data[saved_start_id] = {
            'dest_id': saved_dest_id, 'modifiedTime': saved_modifiedTime,
            'parents': json.loads(saved_parents)
        }

    start_drive_sdk = build('drive', 'v3', credentials=start_creds)
    dest_drive_sdk = build('drive', 'v3', credentials=dest_creds)

    drive_get_params = {
        'fileId': folder_id,
        'fields': 'id,name,modifiedTime',
    }

    logging.config.dictConfig(logging_config_initial)
    logger = logging.getLogger('drive_copier')
    logger.info('About to create {} worker(s) ...'.format(workers_cnt))
    workers = [drivecopyutils.DriveWorker(start_creds_file_name, dest_creds_file_name, unsearched, folder_mapping,
                                          copy_mapping, logger_q, start_id, target_id, max_size,
                                          db_persistency.sqlite_path, already_copied_data, db_lock, scopes)
               for _ in range(workers_cnt)]

    # we get the details of the root folder we want to copy
    root_folder_details = call_endpoint(start_drive_sdk.files().get, drive_get_params)

    # we assume we already performed a copyt
    insert_root_folder = False

    # we check if an old copy is present
    if already_copied_data.get(folder_id):

        drive_check_params = {
            'fileId': already_copied_data[folder_id]['dest_id'],
            'fields': 'id,name,modifiedTime,trashed',
        }
        try:
            drive_check_req = dest_drive_sdk.files().get(**drive_check_params).execute()
        except HttpError:  # somehow the destination root folder does not exist anymore
            insert_root_folder = True
        else:  # the destination root folder is still there
            if drive_check_req.get('trashed'):  # the destination root folder is trashed!
                insert_root_folder = True
            else:  # no exception is triggered, no trash: the destination root folder is still there
                folder_mapping[root_folder_details.get('id')] = already_copied_data[folder_id]['dest_id']
    else:
        insert_root_folder = True

    # if we need to create the destination root folder we do it immediately
    if insert_root_folder:
        # we create the root folder on the destination account
        drive_insert_params = {
            'body': {
                'name': '{} copied from {}'.format(root_folder_details.get('name'), start_id),
                'mimeType': 'application/vnd.google-apps.folder',
            },
            'fields': 'id',
        }
        root_copy = call_endpoint(dest_drive_sdk.files().create, drive_insert_params)

        # we update the folder mapping
        folder_mapping[root_folder_details.get('id')] = root_copy.get('id')

        # there is a new root folder, so we make sure the mapping DB is empty
        db_persistency.reset_mappings()

        # we add the mapping of the root folder to the database
        db_persistency.add_mapping(root_folder_details.get('id'), root_copy.get('id'),
                                   datetime.strptime(root_folder_details.get('modifiedTime'), "%Y-%m-%dT%H:%M:%S.%fZ"),
                                   json.dumps([]))

        # we update the already copied data with the new root mapping
        already_copied_data[root_folder_details.get('id')] = {
            'dest_id': root_copy.get('id'),
            'modifiedTime': datetime.strptime(root_folder_details.get('modifiedTime'), "%Y-%m-%dT%H:%M:%S.%fZ")
        }

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
        if workers[i].is_alive():
            unsearched.put(None)

    stop_event.set()
    listener.join()
    unsearched.join()

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
