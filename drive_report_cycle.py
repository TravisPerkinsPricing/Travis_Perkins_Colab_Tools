parent_folder = "https://drive.google.com/drive/u/0/folders/1aTgfLkc_7xv65HwrSvn4uGECIe_EbvJU"
master_file = "https://docs.google.com/spreadsheets/d/1Pw65YsFwpYnv6h8lcCOEZf8UC6P01WIldVo2z0gpx8o/edit#gid=735534668"

if "drive.google.com" in parent_folder:
    parent_folder = parent_folder.split("/")[-1]

hier = {0 : ("Owner_Type_id", "Owner_Type"),
        1 : ("owner_region", "owner_region_name"),
        2 : ("Owner_Director", "Owner_Director_Name"),
        3 : ("Owner_Code", "Owner_Name")
        }

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

df = pd.io.gbq.read_gbq(
    """
    SELECT distinct Owner_Code, Owner_Name, Owner_Director,
                    Owner_Director_Name, owner_region_name, owner_region,
                    Owner_Type, Owner_Type as Owner_Type_id
    FROM `tp-bi-sandbox.GM_Pricing.GM_Pricing_Customers_TP` LIMIT 50
    """,
    project_id = project_id,
    dialect = "standard")

counter = 0

file_df = pd.DataFrame()
while counter < len(hier):
    if counter == 0:
        #get unique list of top of the hierarchy
        temp_df = df[list(hier[counter])].drop_duplicates()
        for row in temp_df.iterrows():
            #get value to be used as primary key and title
            id_value = row[1][hier[counter][0]]
            title_value = row[1][hier[counter][1]]
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
                if counter + 1 == len(hier):
                    file_id = create_file(master_file, title = title_value,
                                          parent_folder = parent_folder)
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
        all_fields = [list(hier[key]) for key, value in hier.items() if key <= counter]
        all_fields = [item for sublist in all_fields for item in sublist]
        temp_df = df[all_fields]
        #Grab ID fields for this or higher level in hierarchy
        id_fields_needed = [hier[key][0] for key, value, in hier.items() if key <= counter]
        previous_id_fields = [hier[key][0] for key, value, in hier.items() if key < counter]
        #Initalize a new dataframe to replace file_df 
        new_file_df = pd.DataFrame()
        for row in temp_df.iterrows():
            #Extract necessary information
            parent_field_id = row[1][hier[counter - 1][0]]
            previous_level_field = hier[counter - 1][0]
            key = [row[1][i] for i in id_fields_needed]
            key = (str(key).strip("[]")
                           .replace(",", "|")
                           .replace("'", ""))
            previous_key = [row[1][i] for i in previous_id_fields]
            previous_key = (str(previous_key).strip("[]")
                                             .replace(",", "|")
                                             .replace("'", ""))

            id_value = row[1][hier[counter][0]]
            title_value = row[1][hier[counter][1]]
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
                if counter + 1 == len(hier):
                    file_id = create_file(master_file, title = title_value,
                                          parent_folder = parent_folder)
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
        display(file_df)

    counter += 1
