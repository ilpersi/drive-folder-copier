This is a simple tool to allow you copy files between users in different domains without changing the src sharing permissions.

    Usage: drive_copier.py [OPTIONS]
    
      This tool will allow you to copy files and folders between domains without
      modifying the source sharing permissions.
    
    Options:
      -cs, --client-secret <client_secret_json_file>
                                      Path to OAuth2 client secret JSON file.
                                      [default: client_id.json]
      -s, --start-id <start_email_address>
                                      Email address of the source account
                                      [required]
      -t, --target-id <dest_email_address>
                                      Email address of the target account
                                      [required]
      -n, --workers-cnt INTEGER       The number of concurrent processes you want
                                      to run.  [default: 12]
      -id, --folder-id <folder_id>    The id of the folder you want to copy. My
                                      Drive will be used as default  [default:
                                      root]
      -mr, --mapping-report <mapping_csv>
                                      Path where to save the csv output file
                                      containing the copy mapping.  [default:
                                      file_mapping.csv]
      --help                          Show this message and exit.