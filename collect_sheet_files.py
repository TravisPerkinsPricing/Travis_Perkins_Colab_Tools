from google.colab import auth
auth.authenticate_user()
print("Authenticated")
get_ipython().system('pip install --quiet PyDrive')
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
gauth = GoogleAuth()
drive = GoogleDrive(gauth)
credentials, _ = google.auth.default()

def collect_sheet_files(parent_folder):
    if "drive.google.com" in parent_folder:
        parent_folder = parent_folder.split("/")[-1]
    counter = 0
    spreadsheet_list = []
    drive_query = """parents in '{0}'
                    and trashed = false"""

    while True:
        #If crawler is at the top of the folder structure
        if counter == 0 :
            #Get a list of all drive files in the parent folder
            file_list = drive.ListFile(
                        {
                            "q" : drive_query.format(parent_folder)
                        }).GetList()

            folders = []
            #Seperate folders and spreadsheets into lists
            for file in file_list:
                if file["mimeType"].split(".")[-1] == "folder":
                    folders.append(file["id"])
                elif file["mimeType"].split(".")[-1] == "spreadsheet":
                    spreadsheet_list.append(file["id"])
        
        else:

            temp_folders = []
            #Get a list of every Drive file in every drive folder from the previous level
            for folder in folders:
                file_list = drive.ListFile(
                            {
                                "q" : drive_query.format(folder)
                            }).GetList()

                for file in file_list:
                    if file["mimeType"].split(".")[-1] == "folder":
                        temp_folders.append(file["id"])
                    elif file["mimeType"].split(".")[-1] == "spreadsheet":
                        spreadsheet_list.append(file["id"])

            folders = temp_folders

        #If there are no folders at the bottom level, break the loop
        if not folders:
            return spreadsheet_list
            break

        counter += 1
