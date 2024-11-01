# About the script
This repo consists of a script to export the results of a ``PROFILE`` of a set of queries. The results are stored in a tar file that can be easily imported into VerticaPy to view and analyze.

# How to install 
## Prerequisisites

- vsql (make sure that vsql can be directly run from your terminal)
- bash
- `dbadmin` access

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
ADMIN_PASSWORD='""'
```

Note: Above is an example of an empty password. You need nested double quotes inside single quotes.

If you do not want to write your password to file, instead you can create environment variables:

1. Export the passwords:

```
export QUERY_USER_PASSWORD='your_query_user_password'
export ADMIN_PASSWORD='your_admin_password'
```

2. Edit the ``profile_queries.sh`` to include those into them:

```
# Database credentials for query execution
QUERY_USER="ughumman"
QUERY_USER_PASSWORD="${QUERY_USER_PASSWORD}"

# Database credentials for administrative operations
ADMIN_USER="ughumman"
ADMIN_PASSWORD="${ADMIN_PASSWORD}"
```

Provide a local directory where the parquet files can be saved temporarily. The user should have write permissions for this diretory. By default present working directory will be used.

```
LOCAL_DIRECTORY="$PWD"
```

Then optionally add project/customer info:

```
PROJECT_NAME="test"
CUSTOMER_NAME="XYZ"
```

## Using the script

Now you are ready to execute the file from your bash. Note that you need to ``cd`` into the directory where you can see the two executable files (`profile_queries.sh` and `export.sh`)

The script takes two inputs:

1. Job file path

    This file can contain multiple lines. Each line consists of:
    `<User Label> | <User Comment> | <Path to a SQL file>`

    The last line should always be:
    "# END OF FILE - DO NOT REMOVE #"

    Look at the example file `foo.txt`. There is an example for the users to follow.

    ```
    query_batch_1 | random |test_queries.sql
    # END OF FILE - DO NOT REMOVE #
    ```
    In the example above the User Label is "query_batch_1", User Comment is "random", and the SQL file name is "test_queries.sql".

    The SQL files should only contain SQL queries that need to be profiled.

   If the user is not worried about labels and comments, then they can just change the path of SQL file to work with the script.


3. Schema name [Optional]

    This schema name is used to create a schema inside the database where the results are stored temporarily before exporting to a parquet format. If the user does not provide a schema, then a random schema is generated (e.g., `schema_1ec719a09698cc6d`)..

For example:

``./profile_queries.sh --job_file foo.txt --target_schema custom_schema``

OR

``./profile_queries.sh -j foo.txt -s custom_schema``

Note that the schema is optional. You can also just do:

``./profile_queries.sh --job_file foo.txt``

OR

``./profile_queries.sh -j foo.txt``

To get help, simple call:

``./profile_queries.sh --help``

## Output

Once you are done, you will see a tar file with the schema name. Copy this file and send it over to support for further analysis.
