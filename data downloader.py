import paramiko 
from datetime import datetime
import os.path
import time
import glob
from dotenv import load_dotenv
import pandas as pd

load_dotenv()
hostname = os.getenv("HOSTNAME")
username = os.getenv("USER")
password = os.getenv("PASSWORD")
port = os.getenv("PORT")
local = os.getenv("LOCAL")

try:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname, port, username, password)

    sftp = client.open_sftp()
    files = sftp.listdir_attr()
    files_with_dates = [(file.filename, datetime.fromtimestamp(file.st_mtime).date()) for file in files]
    today = [file for file in files_with_dates if file[1] == datetime.today().date()]

    remote = []
    for remote_file in today:
        remote.append(remote_file[0]) 

    for file_r in remote:
        if "Tv" in file_r:
            name = "Tv"
        else:
            name = "Radio"

        if file_r.endswith(".txt"):
            date_file = os.path.splitext(file_r)[0]
            date_file =  datetime.strptime(date_file[-8:], '%Y%m%d').date()
            print(file_r)
            sftp.get(file_r, local + str(date_file) + "_" + name + "_Auditsa.txt")

        if file_r.endswith(".rar"):
            sftp.get(file_r, local + file_r)
            time.sleep(220) ## Wait 200 seconds until the .rar file is download, then extract it.
            import aspose.zip as zp
            with zp.rar.RarArchive(local + file_r) as arc:
                arc.extract_to_directory(local)
            print("Extracted!")

            list_of_files = glob.glob(f'{local}*.txt') # gets lastest .txt
            latest_file = max(list_of_files, key=os.path.getctime)
            print(latest_file)
            
            data = pd.read_csv(latest_file, sep="|")
            data["HIT_Fecha"] = pd.to_datetime(data["HIT_Fecha"])
            
            time.sleep(5)
            try: 
                for n in range(1, 32):
                    filtered = data[data["HIT_Fecha"].dt.day == n]
                    if filtered.empty == False:
                        date = filtered["HIT_Fecha"].iloc[0].strftime('%Y-%m-%d')
                        filtered.to_csv(f"{local}{date}_{name}_Auditsa.csv", index = False, sep="|")
            except:
                print("Error when creating csv files!")

    sftp.close()
    client.close()
except Exception as e:
    print(f"Exception: {e}")
    print(f"Exception type: {type(e).__name__}")
