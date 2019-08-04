# standard imports
import logging
import logging.config
import logging.handlers
import multiprocessing
import pickle

# project imports
from backoff import execute_request

# third parties imports
from google.auth.transport.requests import Request
from googleapiclient.discovery import build


class DriveWorker(multiprocessing.Process):
    def __init__(self, start_creds_file_name, dest_creds_file_name, task_queue, folder_mapping, copy_mapping,
                 log_queue, id_from, id_to, max_size, scopes):
        multiprocessing.Process.__init__(self)

        self.start_creds_file_name = start_creds_file_name
        self.dest_creds_file_name = dest_creds_file_name
        self.task_queue = task_queue
        self.file_mapping = folder_mapping
        self.copy_mapping = copy_mapping
        self.log_queue = log_queue

        self.id_from = id_from
        self.id_to = id_to
        self.max_size = max_size
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
                'fields': 'files(capabilities/canCopy,id,mimeType,name,webViewLink,size),nextPageToken',
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
                child_name = gdrive_child.get('name')
                child_mime_type = gdrive_child.get('mimeType')
                child_size = gdrive_child.get('size')
                child_capabilities = gdrive_child.get('capabilities')
                child_id = gdrive_child.get('id')

                if child_id == start_tmp_id:
                    continue

                if child_mime_type == 'application/vnd.google-apps.folder':
                    gdrive_folders.append(gdrive_child)
                else:
                    if not child_capabilities.get('canCopy') or (child_size > self.max_size > 0):
                        print('Skipping file: {}'.format(gdrive_child))

                        mapping = {
                            'name': child_name,
                            'mimeType': child_mime_type,
                            'size': child_size,
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
                batch_created_folders = []

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

                    insert_request = execute_request(self.dest_drive_sdk.files().create(**drive_insert_params))
                    batch_created_folders.append((folder_id, insert_request))

                # once the batch is over we add new folders to the task queue and we update the mapping
                for original_folder_id, result in batch_created_folders:
                    new_folder = {
                        'id': result.get('id'),
                        'name': result.get('name')
                    }

                    old_folder = {
                        'id': original_folder_id,
                        'name': result.get('name')
                    }

                    mapping = {
                        'name': result.get('name'),
                        'mimeType': 'application/vnd.google-apps.folder',
                        'size': 'N/A',
                        'canCopy': 'N/A',
                        'original-id': original_folder_id,
                        'copy-id': result.get('id'),
                        'original-link':
                            result.get('webViewLink').replace(result.get('id'), original_folder_id),
                        'copy-link': result.get('webViewLink'),
                    }

                    self.file_mapping[original_folder_id] = new_folder.get('id')
                    self.copy_mapping.append(mapping)
                    self.task_queue.put(old_folder)

            # batch temporary copy
            if gdrive_files:
                temporary_file_copies = []
                final_file_copies = []

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
                    tmp_file_copy_request = execute_request(self.start_drive_sdk.files().copy(**tmp_copy_params))

                    # some times file copy is not working correctly and files are not created in the right folder
                    if start_tmp_id not in tmp_file_copy_request.get('parents'):
                        parents_update_params = {
                            'fileId': tmp_file_copy_request.get('id'),
                            'addParents': start_tmp_id,
                            'removeParents': ",".join(tmp_file_copy_request.get('parents')),
                        }

                        execute_request(self.start_drive_sdk.files().update(**parents_update_params))

                    temporary_file_copies.append((file_id, tmp_file_copy_request))

                # once the batch is over, we prepare everything for the final copy and the final deletion
                for original_file_id, result in temporary_file_copies:
                    temp_file_id = result.get('id')
                    temp_file_name = result.get('name')

                    dest_copy_params = {
                        'fileId': temp_file_id,
                        'body': {
                            'name': temp_file_name,
                            'parents': [dest_folder_id],
                        },
                        'fields': 'id,name,mimeType,name,webViewLink,parents,size',
                    }
                    tmp_delete_params = {
                        'fileId': temp_file_id
                    }

                    final_copy_request = execute_request(self.dest_drive_sdk.files().copy(**dest_copy_params))

                    # some times file copy is not working correctly and files are not created in the right folder
                    if dest_folder_id not in final_copy_request.get('parents'):
                        parents_update_params = {
                            'fileId': final_copy_request.get('id'),
                            'addParents': dest_folder_id,
                            'removeParents': ",".join(final_copy_request.get('parents')),
                        }

                        execute_request(self.dest_drive_sdk.files().update(**parents_update_params))

                    final_file_copies.append((original_file_id, final_copy_request))
                    execute_request(self.start_drive_sdk.files().delete(**tmp_delete_params))

                # we update the mapping with the newly copied files
                for original_file_id, result in final_file_copies:
                    mapping = {
                        'name': result.get('name'),
                        'mimeType': result.get('mimeType'),
                        'size': result.get('size'),
                        'canCopy': 'Y',
                        'original-id': original_file_id,
                        'copy-id': result.get('id'),
                        'original-link': result.get('webViewLink').replace(result.get('id'), original_file_id),
                        'copy-link': result.get('webViewLink'),
                    }

                    self.copy_mapping.append(mapping)

            self.task_queue.task_done()

    def __str__(self):
        return "DriveWorker for [{id}] {name}".format(**self.path)


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
