import os
import json
import mosspy
import getpass
from bs4 import BeautifulSoup


mosspy_user_id = 574709399


def evaluatePlagiarismContent(filenames, assignment_id, source):
    '''
    Function used to evaluate plagiarism
    '''
    m = mosspy.Moss(mosspy_user_id, "python")
    # Add all files to be evaluated
    for fname in filenames:
        try:
            m.addFile(fname)
        except:
            pass
    url = m.send()
    # Save result as HTML page
    m.saveWebPage(url, f"../plagiarism/{assignment_id}-{source}.html")
    return url


def getFilePlagiarismURL(assignment_id):
    '''
    Gets plagiarism URL of a file
    '''
    system_username = getpass.getuser()
    home_directory = f"/home/{system_username}"
    team_id_folders = [f for f in os.listdir(
        home_directory) if f.startswith("BD") and "BD_1_2_3_4" not in f]

    mapper_filenames = list()
    reducer_filenames = list()
    spark_task_filenames = list()
    for team_id_folder in team_id_folders:
        if assignment_id.startswith("A3"):
            spark_task_filename = f"{home_directory}/{team_id_folder}/{assignment_id}/script.py"
            spark_task_filenames.append(spark_task_filename)
        else:
            mapper_filename = f"{home_directory}/{team_id_folder}/{assignment_id}/mapper.py"
            reducer_filename = f"{home_directory}/{team_id_folder}/{assignment_id}/reducer.py"
            mapper_filenames.append(mapper_filename)
            reducer_filenames.append(reducer_filename)
    # A3 is spark and hence has no mapper reducer
    if assignment_id.startswith("A3"):
        spark_task_url = evaluatePlagiarismContent(spark_task_filenames, assignment_id, "task")
        return [spark_task_url]
    else:
        mapper_url = evaluatePlagiarismContent(
            mapper_filenames, assignment_id, "mapper")
        reducer_url = evaluatePlagiarismContent(
            reducer_filenames, assignment_id, "reducer")
        return [mapper_url, reducer_url]


def processAnchorTag(anchor):
    link = anchor.get('href')
    filename, similarity = anchor.contents[0].split()
    filename = filename.strip().split('/')[3]
    similarity = float(similarity[1:-2])
    return filename, similarity, link


def getPlagiriasmPercentage(assignment_id, source):
    '''
    Returns plagiarism percentage
    '''
    with open(f"../plagiarism/{assignment_id}-{source}.html") as html_file:
        html = html_file.read()

    soup = BeautifulSoup(html, "html.parser")
    # Finds all the URLS
    anchor_tags = soup.find_all("a")[6:]
    data = dict()

    for i in range(0, len(anchor_tags), 2):
        anchor1 = anchor_tags[i]
        anchor2 = anchor_tags[i+1]

        filename1, similarity1, link1 = processAnchorTag(anchor1)
        filename2, similarity2, link2 = processAnchorTag(anchor2)

        if filename1 not in data:
            data[filename1] = dict()
        if filename2 not in data:
            data[filename2] = dict()

        data[filename1][filename2] = [similarity1, link1]
        data[filename2][filename1] = [similarity2, link2]

    # with open(f"../plagiarism/{assignment_id}-{source}.json", 'w') as json_file:
    #     json.dump(data, json_file, indent=4)

    return data

async def calculatePlagiarismResult(assignment_id):
    try:
        urls = getFilePlagiarismURL(assignment_id)
        if len(urls) > 1:
            mapper_url, reducer_url = urls
            mapper_result = getPlagiriasmPercentage(assignment_id, "mapper")
            reducer_result = getPlagiriasmPercentage(assignment_id, "reducer")
            
            result = {
                "mapper_url": mapper_url,
                "reducer_url": reducer_url,
                "mapper_result": mapper_result,
                "reducer_result": reducer_result
            }
        else:
            task_url = urls[0]
            spark_task_result = getPlagiriasmPercentage(assignment_id, "task")
            result = {
                "script_url": task_url,
                "script_result": spark_task_result
            }

        with open(f"../plagiarism/{assignment_id}.json", 'w') as json_file:
            json.dump(result, json_file, indent=4)

    except ConnectionRefusedError:
        print("Connection to MOSS API refused. Try again later.")
    

def getPlagiarismResult(assignment_id):
    try:
        with open(f"../plagiarism/{assignment_id}.json") as json_file:
            data = json.load(json_file)
    except Exception as error_message:
        data = f"ERROR: {error_message}"
    return data
