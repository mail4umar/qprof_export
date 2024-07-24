# About the script
This repo consists of a script to export the results of a ``PROFILE`` of a set of queries. The results are stored in a tar file that can be easily imported into VerticaPy to view and analyze.

# How to install 
## Prerequisisites

- vsql
- bash
- "dbadmin" access

## Install 
Just copy the entire folder somewhere on your system and make the following two files executable (chmod u+x <file_name>):

1. profile_queries.sh
2. export.sh

# How to use 

## Before using 

Open the ``profile_queries.sh`` file and edit the top section with your credentials:

```
# Database credentials for query execution
QUERY_USER="ughumman"
QUERY_USER_PASSWORD='""'

# Database credentials for administrative operations
ADMIN_USER="ughumman"
ADMIN_PASSWORD=""
```

Provide a local directory where the parquet files can be saved temporarily:

```
LOCAL_DIRECTORY="/scratch_b/ughumman/temp"
```

Then optionally add project/customer info:

```
PROJECT_NAME="test"
CUSTOMER_NAME="XYZ"
```

## Using the script

Now you are ready to execute the file from your bash. Note that you need to ``cd`` inside the directory where you can see the two executable files (profile_queries.sh and export.sh)

The script takes two inputs:

1. Config file path

    This file can contain multiple lines. Each line consists of:
    User Label | User Comment | Path to a SQL file

2. Schema name [Optional]

    This schema name is used to create a schema inside the database where the results are stored temporarily before exporting to a parquet format. If the user does not provide a schema, then a random schema is generated e.g "schema_1ec719a09698cc6d".

For example:

``./profile_queries.sh foo.txt custom_schema``

Note that the schema is optional. You can also just do:

``./profile_queries.sh foo.txt``

## Output

Once you are done, you will see a tar file with the schema name. Copy this file and send it over to support for further analysis.
