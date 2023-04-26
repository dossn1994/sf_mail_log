## Introduction

This code is a Python script that executes a snowflake SQL procedure, retrieves the results as a Pandas DataFrame, converts the DataFrame to an HTML table, and sends an email with the table as an attachment (if available) to a specified recipient. The code also logs the status of the execution process in a file.

## Prerequisites

Before running the code, you will need to have the following:

- Python 3.6 or higher installed on your machine
- Access to a Snowflake account with the necessary privileges to execute the SQL query
- Access to an email account with the necessary privileges to send emails

## Installation

To install the required packages, run the following command in your terminal:

```bash
pip install pandas smtplib snowflake-sqlalchemy sqlalchemy configparser
```

## Configuration

Before running the code, you will need to configure the `config.ini` file with your specific database and email parameters. 

Here is an example of the `config.ini` file:

```ini
[DATABASE]
ACCOUNT = <your_account_name>
USER = <your_user_name>
PASSWORD = <your_password>
DATABASE = <your_database_name>
SCHEMA = <your_schema_name>
WAREHOUSE = <your_warehouse_name>

[EMAIL]
FROM_EMAIL = <your_email_address>
TO_EMAIL = <recipient_email_address>
SMTP_SERVER = <your_smtp_server>
SMTP_PORT = <your_smtp_port>
SMTP_USERNAME = <your_smtp_username>
SMTP_PASSWORD = <your_smtp_password>

[FILE_PATH]
PATH = <path_to_attachment_file>

[LOG]
PATH = <path_to_log_file>
```

## Usage

To run the script, execute the following command in your terminal:

```bash
python script.py
```

The script will execute the snowflake SQL procedure, generate an HTML table, and send an email with the table as an attachment (if available) to the specified recipient. The status of the execution process will be logged in the specified log file.
