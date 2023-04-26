import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from snowflake import connector
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine, text
import configparser
import datetime
import os
config = configparser.ConfigParser()
config.read('config.ini')

def sendmailing(html_table, from_email, to_email, smtp_server, smtp_port, smtp_username, smtp_password,attachment_path=None):
 try:
        # Set up the email parameters
  subject = 'Table'
  if len(html_table) == 0:
   body = f'<html><body><p>No data in table</p></body></html>'
  else:
   body = f'<html><body><p>Table Generated:</p>{html_table}</body></html>'

        # Create the email message
  msg = MIMEMultipart()
  msg['From'] = from_email
  msg['To'] = to_email
  msg['Subject'] = subject
  msg.attach(MIMEText(body, 'html'))
  
  if attachment_path:
   with open(attachment_path, "rb") as attachment:
    part = MIMEApplication(attachment.read(), _subtype=os.path.splitext(attachment_path)[-1][1:])
    part.add_header('Content-Disposition', 'attachment', filename=os.path.basename(attachment_path))
    msg.attach(part)

        # Send the email
  smtp_conn = smtplib.SMTP(smtp_server, smtp_port)
  smtp_conn.starttls()
  smtp_conn.login(smtp_username, smtp_password)
  smtp_conn.sendmail(from_email, to_email, msg.as_string())
  smtp_conn.quit()

  logging_status(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ':  mail sent successfully')

 except Exception as e:
  logging_status(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ':  error occured while sending mail--' + str(e))

def logging_status(message):
 log_path = config['LOG']['PATH']
 newline = "\n************************************************************************************\n"
 finalline = "\n====================================================================================================\n"
 # Open file in append mode
 with open(log_path, 'a') as f:
  if ": Starting process" in message:
   f.write(finalline + newline + message + newline)
  elif ": Process Ended" in message: 
   f.write(newline + message + newline + finalline)
  else:
   f.write(newline + message + newline) 


if __name__ == "__main__":
 try:
  logging_status(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ": Starting process")
    # Read the database connection parameters from config.ini
  account = config['DATABASE']['ACCOUNT']
  user = config['DATABASE']['USER']
  password = config['DATABASE']['PASSWORD']
  database = config['DATABASE']['DATABASE']
  schema = config['DATABASE']['SCHEMA']
  warehouse = config['DATABASE']['WAREHOUSE']

    # Read the email parameters from config.ini
  from_email = config['EMAIL']['FROM_EMAIL']
  to_email = config['EMAIL']['TO_EMAIL']
  smtp_server = config['EMAIL']['SMTP_SERVER']
  smtp_port = config['EMAIL']['SMTP_PORT']
  smtp_username = config['EMAIL']['SMTP_USERNAME']
  smtp_password = config['EMAIL']['SMTP_PASSWORD']
  
  attachment_path = config['FILE_PATH']['PATH']

    # Define the Snowflake connection parameters
  url = URL(
        account=account,
        user=user,
        password=password,
        database=database,
        schema=schema,
        warehouse=warehouse
    )

    # Create an SQLAlchemy engine object
  engine = create_engine(url)

    # Define the SQL query to execute
  query = "CALL SP_TEST_PROC('ron')"

  logging_status(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ": Generating table")

    # Execute the SQL query and retrieve the results as a Pandas DataFrame
  df = pd.read_sql_query(query, engine)

    # Convert the DataFrame to an HTML table
  if df.empty:
   logging_status(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ": Empty table generated")
   sendmailing('', from_email, to_email, smtp_server, smtp_port, smtp_username, smtp_password)
  else:
   html_table = df.to_html(index=False)
   logging_status(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ": Table generated")
   sendmailing(html_table, from_email, to_email, smtp_server, smtp_port, smtp_username, smtp_password,attachment_path)

    # Close the Snowflake connection
  engine.dispose()

 except Exception as e:
  logging_status(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ': Exception occured--' + str(e))
 else:
  logging_status(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ": Process Ended")