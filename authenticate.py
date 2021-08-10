def authenticate_me(bq_project_id):
  # Authenticate the user
  from google.colab import auth
  auth.authenticate_user()
  print("Authenticated")
  from googleapiclient.discovery import build
  #sheets_service = build('sheets', 'v4')
  drive_service = build('drive', 'v3')

  #set parameters for BigQuery and display tables
  %load_ext google.colab.data_table
  project_id = bq_project_id

  get_ipython().system('pip install --quiet PyDrive')
  import google.auth
  from oauth2client.client import GoogleCredentials
  from pydrive.auth import GoogleAuth
  from pydrive.drive import GoogleDrive
  gauth = GoogleAuth()
  drive = GoogleDrive(gauth)
  gauth.credentials = GoogleCredentials.get_application_default()
  import gspread
  get_ipython().system('pip install --quiet gspread-pandas')
  gc = gspread.authorize(GoogleCredentials.get_application_default())
  credentials, _ = google.auth.default()
  from google.colab import auth
  #Install necessary packages into session
  get_ipython().system('pip install --upgrade --quiet pandas-gbq')
  import pandas_gbq
  pandas_gbq.Context.project = project_id
  get_ipython().system('pip install --upgrade --quiet gspread')
  get_ipython().system('pip install --upgrade --quiet pygsheets')
  import pygsheets
  pyc = pygsheets.client.Client(credentials)
