def get_github_access_token():
    # Authenticate the session
    from google.colab import auth
    auth.authenticate_user()
    print("Authenticated")
    from googleapiclient.discovery import build
    #sheets_service = build('sheets', 'v4')
    drive_service = build('drive', 'v3')
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
    get_ipython().system('pip install --upgrade --quiet pandas-gbq')
    get_ipython().system('pip install --upgrade --quiet gspread')
    get_ipython().system('pip install --upgrade --quiet pygsheets')
    import pygsheets
    pyc = pygsheets.client.Client(credentials)
    import requests
    gcloud_token = get_ipython().system('gcloud auth print-access-token')
    gcloud_tokeninfo = requests.get('https://www.googleapis.com/oauth2/v3/tokeninfo?access_token=' + gcloud_token[0]).json()
    print(gcloud_tokeninfo['email'])

    drive_query = """fullText contains '{0}'
                        and '{1}' in owners
                        and trashed = false"""

    text_to_check = "GitHub_access_token"
    owner = gcloud_tokeninfo['email']

    #Look for list of files that satisfies the query
    access_token_file = drive.ListFile(
        {
            "q" : drive_query.format(text_to_check, owner)
        }).GetList()[0]

    access_token_file["id"]

    gh_access_token = (pyc.open_by_key(access_token_file["id"])
                        .worksheet_by_title("Sheet1")
                        .cell("A1")
                        .value
                        )
    
    return gh_access_token
