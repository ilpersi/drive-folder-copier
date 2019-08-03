# standard imports
import json
import logging
import logging.config
import logging.handlers
import multiprocessing
import os
import pickle

import tenacity

from src.backoff import execute_request, before_sleep_log

# third parties imports
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient import errors

MAX_ATTEMPTS = 30
EXP_MULTIPLIER = 0.5
EXP_MAX_WAIT = 60


class DriveWorker(multiprocessing.Process):
    def __init__(self, start_creds_file_name, dest_creds_file_name, task_queue, folder_mapping, copy_mapping,
                 log_queue, id_from, id_to, scopes):
        multiprocessing.Process.__init__(self)

        self.start_creds_file_name = start_creds_file_name
        self.dest_creds_file_name = dest_creds_file_name
        self.task_queue = task_queue
        self.file_mapping = folder_mapping
        self.copy_mapping = copy_mapping
        self.log_queue = log_queue

        self.id_from = id_from
        self.id_to = id_to
        self.scopes = scopes

        # Properties used outside the init
        self.start_drive_sdk = None
        self.dest_drive_sdk = None
        self.path = {'id': None, 'name': None}
        self.logger = None

    def run(self):

        # START ID CREDENTIAL FLOW
        with open(self.start_creds_file_name, 'rb') as credentials_dat:
            start_creds = pickle.load(credentials_dat)

        # DEST ID CREDENTIAL FLOW
        with open(self.dest_creds_file_name, 'rb') as credentials_dat:
            dest_creds = pickle.load(credentials_dat)

        self.start_drive_sdk = build('drive', 'v3', credentials=start_creds)
        self.dest_drive_sdk = build('drive', 'v3', credentials=dest_creds)

        config_worker = {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'detailed': {
                    'class': 'logging.Formatter',
                    'format': '%(asctime)s %(name)-15s %(levelname)-8s %(processName)-10s %(message)s'
                }
            },
            'handlers': {
                'queue': {
                    'class': 'logging.handlers.QueueHandler',
                    'queue': self.log_queue,
                },
                'console': {
                    'class': 'logging.StreamHandler',
                    'level': 'DEBUG',
                },
            },
            'root': {
                'level': 'DEBUG',
                'handlers': ['queue']
            },
            self.name: {
                'level': 'DEBUG',
                'handlers': ['console'],
                'formatter': 'detailed'
            },
        }
        logging.config.dictConfig(config_worker)
        self.logger = logging.getLogger(self.name)

        while True:

            if start_creds.expired:
                start_creds.refresh(Request())

            if dest_creds.expired:
                dest_creds.refresh(Request())

            next_task = self.task_queue.get()

            if next_task is None:
                # Poison pill means shutdown
                print('{}: Exiting'.format(self.name))

                self.task_queue.task_done()
                break

            self.logger.info(next_task)
            start_folder_id = next_task.get('id')
            # start_folder_full_name = next_task.get('name')
            dest_folder_id = self.file_mapping.get(start_folder_id)
            start_tmp_id = self.file_mapping.get('root_copy_tmp')

            page_size = 1000
            drive_list_params = {
                'pageSize': page_size,
                'q': "'{}' in parents and trashed=false".format(start_folder_id),
                'orderBy': 'name',
                'fields': 'files(capabilities/canCopy,id,mimeType,name,webViewLink),nextPageToken',
            }

            folder_childs = []

            files = self.start_drive_sdk.files()
            request = files.list(**drive_list_params)
            while request is not None:
                current_files = execute_request(request)
                folder_childs.extend(current_files.get('files', []))
                request = files.list_next(request, current_files)

            gdrive_folders = []
            gdrive_files = []

            # we separate google drive folders from files in two different lists
            for gdrive_child in folder_childs:
                child_capabilities = gdrive_child.get('capabilities')
                child_id = gdrive_child.get('id')
                child_name = gdrive_child.get('name')
                child_mime_type = gdrive_child.get('mimeType')

                if child_id == start_tmp_id:
                    continue

                if child_mime_type == 'application/vnd.google-apps.folder':
                    gdrive_folders.append(gdrive_child)
                else:
                    if not child_capabilities.get('canCopy'):
                        print('Skipping file: {}'.format(gdrive_child))

                        mapping = {
                            'name': child_name,
                            'mimeType': child_mime_type,
                            'canCopy': 'N',
                            'original-id': child_id,
                            'copy-id': '',
                            'original-link': gdrive_child.get('webViewLink'),
                            'copy-link': '',
                        }
                        self.copy_mapping.append(mapping)

                        continue
                    gdrive_files.append(gdrive_child)

            # batch folder copy
            if gdrive_folders:  # if there are no folders, we skip
                batch_folder_creation = DriveBatchFileManager(self.dest_drive_sdk)
                for gdrive_folder in gdrive_folders:
                    folder_id = gdrive_folder.get('id')
                    folder_name = gdrive_folder.get('name')

                    if self.file_mapping.get(folder_id):
                        print("Folder {} already copyied".format(folder_id))
                        continue

                    drive_insert_params = {
                        'body': {
                            'name': folder_name,
                            'mimeType': 'application/vnd.google-apps.folder',
                            'parents': [dest_folder_id],
                        },
                        'fields': 'id,name,webViewLink',
                    }
                    batch_folder_creation.add(self.dest_drive_sdk.files().create, drive_insert_params, folder_id)

                batch_folder_creation.execute()
                # once the batch is over we add new folders to the task queue and we update the mapping
                for original_folder_id, result in batch_folder_creation.results.items():
                    response = result.get('response', False)
                    if response:
                        new_folder = {
                            'id': response.get('id'),
                            'name': response.get('name')
                        }

                        old_folder = {
                            'id': original_folder_id,
                            'name': response.get('name')
                        }

                        mapping = {
                            'name': response.get('name'),
                            'mimeType': 'application/vnd.google-apps.folder',
                            'canCopy': 'N/A',
                            'original-id': original_folder_id,
                            'copy-id': response.get('id'),
                            'original-link': response.get('webViewLink')
                                .replace(response.get('id'), original_folder_id),
                            'copy-link': response.get('webViewLink'),
                        }

                        self.file_mapping[original_folder_id] = new_folder.get('id')
                        self.copy_mapping.append(mapping)
                        self.task_queue.put(old_folder)

            # batch temporary copy
            if gdrive_files:
                batch_file_temp_copy = DriveBatchFileManager(self.start_drive_sdk, 3)
                for gdrive_file in gdrive_files:
                    file_id = gdrive_file.get('id')
                    file_name = gdrive_file.get('name')

                    tmp_copy_params = {
                        'fileId': file_id,
                        'body': {
                            'name': file_name,
                            'parents': [start_tmp_id],
                        },
                        'fields': 'id,name,parents',
                    }
                    batch_file_temp_copy.add(self.start_drive_sdk.files().copy, tmp_copy_params, file_id)

                batch_file_temp_copy.execute()

                # once the batch is over, we prepare everything for the final copy and the final deletion
                batch_file_final_copy = DriveBatchFileManager(self.dest_drive_sdk, 3)
                batch_file_temp_delete = DriveBatchFileManager(self.start_drive_sdk)
                for original_file_id, result in batch_file_temp_copy.results.items():
                    temp_response = result.get('response', False)
                    temp_file_id = temp_response.get('id')
                    temp_file_name = temp_response.get('name')

                    if temp_response:
                        dest_copy_params = {
                            'fileId': temp_file_id,
                            'body': {
                                'name': temp_file_name,
                                'parents': [dest_folder_id],
                            },
                            'fields': 'id,name,mimeType,name,webViewLink',
                        }
                        tmp_delete_params = {
                            'fileId': temp_file_id
                        }
                        batch_file_final_copy.add(self.dest_drive_sdk.files().copy, dest_copy_params, original_file_id)
                        batch_file_temp_delete.add(self.start_drive_sdk.files().delete, tmp_delete_params, temp_file_id)

                batch_file_final_copy.execute()
                batch_file_temp_delete.execute()

                # we update the mapping with the newly copied files
                for original_file_id, result in batch_file_final_copy.results.items():
                    response = result.get('response', False)
                    if response:
                        mapping = {
                            'name': response.get('name'),
                            'mimeType': response.get('mimeType'),
                            'canCopy': 'Y',
                            'original-id': original_file_id,
                            'copy-id': response.get('id'),
                            'original-link': response.get('webViewLink').replace(response.get('id'), original_file_id),
                            'copy-link': response.get('webViewLink'),
                        }

                        self.copy_mapping.append(mapping)

            self.task_queue.task_done()

    def __str__(self):
        return "DriveWorker for [{id}] {name}".format(**self.path)


class DriveBatchFileManager:
    def __init__(self, service, max_batch_size=100):

        self._service = service
        self.max_batch_size = max_batch_size
        self.results = {}

        self._batch_list = [self._service.new_batch_http_request(callback=self._manage_responses), ]
        self._request_cnt = 0
        self._id_cnt = 0
        self._id_mapping = {}

        self._errors = []

    def add(self, end_point, params, request_id, add_to_cache=True):

        internal_id = str(self._id_cnt)
        self._id_mapping[internal_id] = request_id
        request = end_point(**params)

        if add_to_cache:
            if not self.results.get(request_id, False):
                self.results[request_id] = {}
            self.results[request_id]['end_point'] = end_point
            self.results[request_id]['params'] = params

        current_batch = (self._request_cnt // self.max_batch_size)

        if len(self._batch_list) < (current_batch + 1):
            self._batch_list.append(self._service.new_batch_http_request(callback=self._manage_responses))

        self._batch_list[current_batch].add(request, request_id=internal_id)
        self._request_cnt += 1
        self._id_cnt += 1

    @tenacity.retry(stop=tenacity.stop_after_attempt(MAX_ATTEMPTS),
                    wait=tenacity.wait_exponential(multiplier=EXP_MULTIPLIER, max=EXP_MAX_WAIT),
                    before_sleep=before_sleep_log)
    def execute(self):

        for batch_request in self._batch_list:
            batch_request.execute()

        if len(self._errors) != 0:
            self._batch_list = [self._service.new_batch_http_request(callback=self._manage_responses), ]
            self._request_cnt = 0

            for request_error in self._errors:
                request_exeption = request_error['exception']
                internal_id = request_error['internal_id']
                request_id = self._id_mapping[internal_id]

                cache_element = self.results[request_id]
                new_end_point = cache_element['end_point']
                new_params = cache_element['params']

                if isinstance(request_exeption, errors.HttpError):
                    if isinstance(request_exeption.content, str):
                        error_contet = request_exeption.content
                    else:
                        error_contet = request_exeption.content.decode('utf-8')
                    error_details = json.loads(error_contet).get('error')
                    error_code = error_details.get('code', 0)
                    error_message = error_details.get('message', '')

                    if error_code == 404:
                        logging.debug('File not found when processing batch request')
                        logging.debug(request_exeption)
                    else:
                        logging.debug('error_code: {}, error_message: {}'.format(error_code, error_message))
                        self.add(new_end_point, new_params, request_id, False)

                else:
                    logging.warning('Unexpecte Exception Type: {}'.format(request_exeption))

            self._errors = []
            raise errors.BatchError('At least one item of a batch request returned an error')

    def _manage_responses(self, internal_id, response, exception):
        if exception is not None:
            self._errors.append(
                {
                    'internal_id': internal_id,
                    'response': response,
                    'exception': exception,
                }
            )
        else:
            request_id = self._id_mapping[internal_id]
            self.results[request_id]['response'] = response


class LoggingListener(multiprocessing.Process):
    def __init__(self, logging_q, stop_event, id_from, id_to):
        multiprocessing.Process.__init__(self)

        self.logging_q = logging_q
        self.stop_event = stop_event
        self.id_from = id_from
        self.id_to = id_to

    def run(self):
        listener_logging_config = {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'detailed': {
                    'class': 'logging.Formatter',
                    'format': '%(asctime)s %(name)-15s %(levelname)-8s %(processName)-10s %(message)s'
                },
                'simple': {
                    'class': 'logging.Formatter',
                    'format': '%(name)-15s %(levelname)-8s %(processName)-10s %(message)s'
                }
            },
            'handlers': {
                'console': {
                    'class': 'logging.StreamHandler',
                    'level': 'WARNING',
                    'formatter': 'simple',
                },
                'file': {
                    'class': 'logging.FileHandler',
                    'filename': '{}-{}.log'.format(self.id_from, self.id_to),
                    'mode': 'w',
                    'formatter': 'detailed',
                    'encoding': 'utf-8',
                },
                'googleapiclient.discovery': {
                    'class': 'logging.FileHandler',
                    'filename': 'googleapiclient.discovery.log',
                    'mode': 'w',
                    'formatter': 'detailed',
                    'encoding': 'utf-8',
                }
            },
            'root': {
                'level': 'DEBUG',
                'handlers': ['console', 'file']
            },
            'loggers': {
                'googleapiclient.discovery': {
                    'level': 'INFO',
                    'handlers': ['googleapiclient.discovery', 'file'],
                    'propagate': False,
                },
            }
        }

        logging.config.dictConfig(listener_logging_config)
        listener = logging.handlers.QueueListener(self.logging_q, LoggingHandler())
        listener.start()
        self.stop_event.wait()
        listener.stop()


class LoggingHandler:
    """
    A simple handler for logging events. It runs in the listener process and
    dispatches events to loggers based on the name in the received record,
    which then get dispatched, by the logging system, to the handlers
    configured for those loggers.
    """

    @staticmethod
    def handle(record):
        logger = logging.getLogger(record.name)
        # The process name is transformed just to show that it's the listener
        # doing the logging to files and console
        record.processName = '{} (for {})'.format(multiprocessing.current_process().name, record.processName)
        logger.handle(record)
