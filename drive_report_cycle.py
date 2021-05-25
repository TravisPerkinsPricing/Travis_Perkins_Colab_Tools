import pandas as pd
from googleapiclient.discovery import build
from pydrive.drive import GoogleDrive
import pygsheets
drive_service = build('drive', 'v3')
credentials, _ = google.auth.default()
pyc = pygsheets.client.Client(credentials)
drive = GoogleDrive(gauth)

def check_existence(parent_folder, text_to_check):
    #Included repeating try except becuase the Google API sometimes fails for no reason
    i = 0
    while True:
        try:
            drive_query = """fullText contains '{0}'
                            and parents in '{1}'
                            and trashed = false"""

            #Look for list of files that satisfies the query
            file_exists = drive.ListFile(
                {
                    "q" : drive_query.format(text_to_check, parent_folder)
                }).GetList()
            
            break

        except Exception as e:
            if i < 4:
                i+=4
                continue
            else:
                raise e

    return file_exists

def create_folder(id, title, parent_folder):
    #Included repeating try except becuase the Google API sometimes fails for no reason
    i = 0
    while True:
        try:
            folder = drive.CreateFile(
                {"title" : title,
                "mimeType" : "application/vnd.google-apps.folder",
                "parents"  : [{"kind": "drive#fileLink",
                                "id" : parent_folder}],
                "description" : id}
            )

            #Upload folder object to Google Drive
            folder.Upload()

            break

        except Exception as e:
            if i < 4:
                i+=4
                continue
            else:
                raise e

    return folder

def create_file(master_file, title, parent_folder):
    #Included repeating try except becuase the Google API sometimes fails for no reason
    i = 0
    while True:
        try:
            #Copy the master google sheets file
            sh = gc.copy(master_file, title = title)
            file_id = sh.id
            folder_id = parent_folder
            # Retrieve the existing parents to remove
            file = drive_service.files().get(fileId=file_id,
                                            fields = "parents").execute()
            previous_parents = ",".join(file.get("parents"))
            # Move the file to the new folder
            file = drive_service.files().update(fileId = file_id,
                                                addParents = folder_id,
                                                removeParents = previous_parents,
                                                fields="id, parents").execute()

            break
        
        except Exception as e:
            if i < 4:
                i+=4
                continue
            else:
                raise e
            
    return file_id

def populate_file(file_id, sheets_object, filter):
    #Open session for file
    session = pyc.open_by_key(file_id)
    #Set filtered dataframe in each sheet
    for sheet, _config in sheets_object.items():
        tab = session.worksheet_by_title(sheet)
        field = filter["field"]
        value = filter["value"]
        data = _config["df"][_config["df"][field] == value].drop("end_key", axis = 1)
        tab.set_dataframe(data, start = _config["start_pos"])
    #Hide sheets that are not mentioned in the sheets object
    for i in session.worksheets():
        if i.title not in [key for key, value in sheets_object.items()]:
            i.hidden = True

def report_cycle(parent_folder, master_file, sheets_object, df,
                 hierarchy_object):
    if "drive.google.com" in parent_folder:
        parent_folder = parent_folder.split("/")[-1]

    if "docs.google.com" in master_file:
        master_file = master_file.split("/")[5]

    df["end_key"] = df[[value[0] for key, value in hierarchy_object.items()]].agg("| ".join, axis = 1)

    counter = 0

    file_df = pd.DataFrame()
    while counter < len(hierarchy_object):
        if counter == 0:
            #get unique list of top of the hierarchy
            temp_df = df[list(hierarchy_object[counter])].drop_duplicates()
            for row in temp_df.iterrows():
                #get value to be used as primary key and title
                id_value = row[1][hierarchy_object[counter][0]]
                title_value = row[1][hierarchy_object[counter][1]]
                #Check if folder exists with primary key in description
                file_exists = check_existence(parent_folder, id_value)

                if file_exists:
                    #Append parent info and folder ID with temp dataframe
                    file_df  = file_df.append(
                        pd.DataFrame({"parent_folder"  : [parent_folder],
                                    "file_id"        : [file_exists[0]["id"]],
                                    "key"            : id_value})
                        )

                else:
                    #Check if this item is the last in the hierarchy
                    if counter + 1 == len(hierarchy_object):
                        file_id = create_file(master_file, title = title_value,
                                            parent_folder = parent_folder)
                        populate_file(file_id, sheets_object,
                                    filter = {"field" : hierarchy_object[counter[0]],
                                                "value" : id_value})

                    else:
                        #Create folder and capture folder ID
                        folder = create_folder(id_value, title_value, parent_folder)
                        #Append parent info and folder ID with temp dataframe
                        file_df = file_df.append(
                            pd.DataFrame({"parent_folder"  : [parent_folder],
                                        "file_id"        : [folder["id"]],
                                        "key"            : [id_value]})
                        )

        else:
            #Grab fields from dataframe at this or higher level in hierarchy
            all_fields = [list(hierarchy_object[key]) for key, value in hierarchy_object.items() if key <= counter]
            all_fields = [item for sublist in all_fields for item in sublist]
            temp_df = df[all_fields]
            #Grab ID fields for this or higher level in hierarchy
            id_fields_needed = [hierarchy_object[key][0] for key, value, in hierarchy_object.items() if key <= counter]
            previous_id_fields = [hierarchy_object[key][0] for key, value, in hierarchy_object.items() if key < counter]
            #Initalize a new dataframe to replace file_df 
            new_file_df = pd.DataFrame()
            for row in temp_df.iterrows():
                #Extract necessary information
                parent_field_id = row[1][hierarchy_object[counter - 1][0]]
                previous_level_field = hierarchy_object[counter - 1][0]
                key = [row[1][i] for i in id_fields_needed]
                key = (str(key).strip("[]")
                            .replace(",", "|")
                            .replace("'", ""))
                previous_key = [row[1][i] for i in previous_id_fields]
                previous_key = (str(previous_key).strip("[]")
                                                .replace(",", "|")
                                                .replace("'", ""))

                id_value = row[1][hierarchy_object[counter][0]]
                title_value = row[1][hierarchy_object[counter][1]]
                parent_folder = file_df[file_df["key"] == previous_key]["file_id"].values[0]

                file_exists = check_existence(parent_folder, id_value)

                if file_exists:
                    #append this row's data to new dataframe
                    new_file_df = new_file_df.append(
                        pd.DataFrame({"parent_folder"  : [parent_folder],
                                    "file_id"        : [file_exists[0]["id"]],
                                    "key"            : [key]})
                    )

                else:
                    #Check if this item is the last in the hierarchy
                    if counter + 1 == len(hierarchy_object):
                        file_id = create_file(master_file, title = title_value,
                                            parent_folder = parent_folder)
                        
                        populate_file(file_id, sheets_object, 
                                    filter = {"field" : "end_key",
                                                "value" : key})
                        
                    else:
                        #Create folder and capture folder ID
                        folder = create_folder(id_value, title_value, parent_folder)
                        #Append parent info and folder ID with temp dataframe
                        new_file_df = new_file_df.append(
                            pd.DataFrame({"parent_folder"  : [parent_folder],
                                        "file_id"        : [folder["id"]],
                                        "key"            : [key]})
                        )

            file_df = new_file_df.copy()

        counter += 1
