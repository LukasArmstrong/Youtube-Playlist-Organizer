import mariadb
import datetime as dt
import pickle
import re
import os
import operator
from icecream import ic
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from natsort import natsorted, ns
import yaml
#
def getProjectVariables(file):
    with open(file, 'r') as f:
        projectVariables = yaml.safe_load(f)
    return tuple(projectVariables.values())

#MariaDB/SQL functions to store data
def getDataBaseConnection(usr, pswd, host, port,db):
    try:
        conn = mariadb.connect(
            user = usr,
            password = pswd,
            host = host,
            port = port,
            database = db
        )
    except mariadb.Error as e:
        raise ValueError(f"Error connecting to MariaDB Platform: {e}")
    return conn

def getDataDB(conn, tableString, *cols):
    cur = conn.cursor()
    query = "Select " + " ,".join(cols) +" from " + tableString
    cur.execute(query)
    return cur.fetchall()

def getWatchLaterDB(conn):
    cur = conn.cursor()
    cur.execute(
        "Select * from WatchLaterList"
    )
    watchlater = []
    for item in cur:
        watchlater.append(item[0],item[1],item[2],item[3],item[4],item[5],item[6])
    return watchlater

def insertTime(conn, creatorID, dateTimeString, videoID):
    cur = conn.cursor()
    cur.execute(
        "Insert Into uploadTimes(creatorID, publishTime, videoID) Values (?,?,?)", (creatorID, dateTimeString, videoID)
    )
    conn.commit()

def insertCreator(conn, creatorString, channelIDString, priorityScore=0):
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO Creators(creators,priorityScore,channelId) Values(?,?,?)", (creatorString, priorityScore, channelIDString)
    )
    conn.commit()

def storeWatchLater(conn, watchlater):
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM WatchLaterList"
    )
    conn.commit()
    for video in watchlater:
        cur.execute(
            "INSERT INTO WatchLaterList(position, playlistID, videoID, duration, creator, publishedTimeUTC, title) VALUE (?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE position=Value(position)", (video[0], video[1], video[2], video[3], video[4], video[5], video[6])
        )
        conn.commit()

#Youtube API
def getCredentials(portNumber, clientSecretFile="client_secret-Youtube_GhostTheToast.json"):
    credentials =  None
    # token.pickle stores the user's credentials from previously successful logins
    if os.path.exists("token.pickle"):
        print("Loading credentials from file...")
        with open("token.pickle", "rb") as token:
            credentials = pickle.load(token)

    #If there is no valid credentials available, then either refresh the token or log in.
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            print("Refreshing access token...")
            credentials.refresh(Request())
            saveCredentails(credentials)
        else:
            print("Fetching new token...")
            flow = InstalledAppFlow.from_client_secrets_file(
                clientSecretFile,
                scopes=["https://www.googleapis.com/auth/youtube",
                        "https://www.googleapis.com/auth/youtube.force-ssl", 
                        "https://www.googleapis.com/auth/youtubepartner"]
            )
            flow.run_local_server(
                port=portNumber, 
                prompt="consent", 
                authorization_prompt_message=""
            )
            credentials = flow.credentials
            saveCredentails(credentials)
    return credentials

def getFlowObject(clientSecretFile="client_secret-Youtube_GhostTheToast.json"):
     return InstalledAppFlow.from_client_secrets_file(
        clientSecretFile,
        scopes=["https://www.googleapis.com/auth/youtube",
                "https://www.googleapis.com/auth/youtube.force-ssl", 
                "https://www.googleapis.com/auth/youtubepartner"]
    )
    #flow.run_local_server(
    #    port=portNumber, 
    #    prompt="consent", 
    #    authorization_prompt_message=""
    #)

def saveCredentails(credentials):
    # Save credentials for the next run
    with open("token.pickle", "wb") as f:
        print("Saving credentials for future use...")
        pickle.dump(credentials, f)

def getWatchLater(youtube, playlistID, nextPageBoolean):
    nextPageToken = None
    numberRequest = 0
    watchLaterList = []
    try:
        while True:
            pl_request = youtube.playlistItems().list(
                part="contentDetails, snippet",
                playlistId=playlistID,
                maxResults = 50,
                pageToken = nextPageToken
            )

            pl_response = pl_request.execute()
            numberRequest += 1

            for item in pl_response["items"]:
                video = (item["snippet"]["position"], item["id"], item["contentDetails"]["videoId"])
                vid_request = youtube.videos().list(
                    part="contentDetails, snippet",
                    id = item["contentDetails"]["videoId"],
                )
                vid_response = vid_request.execute()
                for vid in vid_response["items"]:
                    duration = durationString2Sec(vid["contentDetails"]["duration"])
                    utcPublishedTime =  dateString2EpochTime(vid["snippet"]["publishedAt"])
                    videoSnippet = (duration, vid["snippet"]["channelTitle"], utcPublishedTime, vid["snippet"]["title"])
                video = video + videoSnippet
                watchLaterList.append(video)

            if nextPageBoolean:    
                nextPageToken = pl_response.get('nextPageToken')

            if not nextPageToken:
                break
    except:
        print("Failed to get Watch Later :(")
    return watchLaterList, numberRequest
        
def updatePlaylist(watchLater, sortedWatchLater, youtube, playlistID):
    numOperations = 0
    if len(watchLater) != len(sortedWatchLater):
        raise ValueError("List must have the same size")
    for x in range(len(watchLater)): 
        if watchLater[x] != sortedWatchLater[x]:
            update_request = youtube.playlistItems().update(
                part = "snippet",
                body={
                    "id": sortedWatchLater[x][1],
                    "snippet": {
                        "playlistId": playlistID,
                        "position": x,
                        "resourceId": {
                        "kind": "youtube#video",
                        "videoId": sortedWatchLater[x][2],
                        }
                    }
                }
            )
            update_response = update_request.execute()
            numOperations += 1
            watchLater.insert(0, watchLater.pop(watchLater.index(sortedWatchLater[x])))
            print(update_response["snippet"]["title"])
    print("Number of operations preformed: " + str(numOperations))
    return numOperations, watchLater

def getVideoYT(youtube, videoID):
    vid_request = youtube.videos().list(
        part="contentDetails, snippet",
        id = videoID,
    )
    vid_response = vid_request.execute()
    videoDetails = []
    for vid in vid_response["items"]:
        videoDetails = {
            "duration" : vid["contentDetails"]["duration"],
            "creator" :  vid["snippet"]["channelTitle"], 
            "published" : vid["snippet"]["publishedAt"],
            "title" : vid["snippet"]["title"]
        }
    return videoDetails

def insertVideo2WatchLater(conn, youtube, playlistID, videoID):
    watchLater = getWatchLaterDB(conn)
    videoDetails = getVideoYT(youtube,videoID)
    watchLater.append(len(watchLater), '', videoID, durationString2Sec(videoDetails["duration"]), videoDetails["creator"], dateString2EpochTime(videoDetails["published"]), videoDetails["title"])
    sortedWatchLater = sortWatcherLater(watchLater,)
    updatePlaylist(youtube, playlistID, watchLater, sortedWatchLater)

#Time Math
def durationString2Sec(durationString, hours_pattern=re.compile(r'(\d+)H'), minutes_pattern=re.compile(r'(\d+)M'), seconds_pattern=re.compile(r'(\d+)S')):
    hoursString = hours_pattern.search(durationString)
    minutesString = minutes_pattern.search(durationString)
    secondsString = seconds_pattern.search(durationString)

    hours = int(hoursString.group(1)) if hoursString else 0
    minutes = int(minutesString.group(1)) if minutesString else 0
    seconds = int(secondsString.group(1)) if secondsString else 0

    video_seconds = dt.timedelta(
        hours=hours,
        minutes=minutes,
        seconds=seconds
    ).total_seconds()
    return video_seconds

def dateString2EpochTime(dateString, time_pattern="%Y-%m-%dT%H:%M:%SZ"):
    d = dt.datetime.strptime(dateString, time_pattern)
    epoch = dt.datetime(1970,1,1)
    return (d-epoch).total_seconds()

def filterDict(dict, string, threshold): 
    ops = {
        "<" : operator.ge,
        "<=" : operator.gt,
        ">" : operator.le,
        ">=" : operator.lt
    }
    tempDict = dict.copy()
    for key, value in dict.items():
        if ops[string](value,threshold):
            del tempDict[key]
    return tempDict

def printMultiList(*args):
    for list_ in args:
        for element in list_:
            print(element, end=' ')
        print('\n')
    


def sortWatcherLater(watchLaterList, creatorDict, keywordDict, numSerKeywords, serKeywords, videoFollowUpList):
    #WatchLaterList structured as (position on yt, playlist id for yt, video id for yt, duration in seconds, creator, published time in unix time, video title)
    workingWatchLater = watchLaterList.copy()
    videoCountThreshold = 50 #Number of items in list to determine priority limit
    durationThreshold = 41*60 #41 minutes in seconds / Wanted to include anything that is 40 minutes + change and under. Main goal is to stop super long content from choking up the priority queue.
    #How priority is defined
    if len(watchLaterList) > videoCountThreshold:
        priorityThreshold = 0
    else:
        priorityThreshold = 1
    creatorDict = filterDict(creatorDict, ">", priorityThreshold)
    keywordDict = filterDict(keywordDict, ">", priorityThreshold)
    setValues = set(list(creatorDict.values()))
    setValues.update(list(keywordDict.values()))
    scoreSet = sorted(list(setValues), reverse=True)
    patternString = r"(%s)\s\d+" % "|".join(numSerKeywords) + "|(%s)" % "|".join(serKeywords)
    seriesPattern = re.compile(patternString, re.IGNORECASE)
    priorityWatchLater = [[] for i in range(len(scoreSet))]
    seriesList = []
    #Step 1 Find all and pull out priority videos into new list
    for item in watchLaterList:
        result = seriesPattern.search(item[6])
        if result:
            if 'Smarter Every Day' in item[6]:
                list_item = list(item)
                list_item[6] = item[6][result.start():]
                seriesList.append(list_item)
            else:
                seriesList.append(list(item))
        keywordFound = False
        for word in keywordDict.keys():
            if word in item[6]:
                if keywordDict[word] in scoreSet:
                    scoreIndex = scoreSet.index(keywordDict[word])
                    priorityWatchLater[scoreIndex].append(item)
                    workingWatchLater.remove(item)
                    keywordFound = True
                    break
        if keywordFound:
            continue
        if item[4] in creatorDict.keys():
            if item[3]<(durationThreshold):
                if creatorDict[item[4]] in scoreSet:
                    scoreIndex = scoreSet.index(creatorDict[item[4]])
                    priorityWatchLater[scoreIndex].append(item)
                    workingWatchLater.remove(item)
    sortedwatchLaterList = sorted(workingWatchLater, key=lambda x: (x[3], x[5]))
    sortedSeriesList = natsorted(seriesList, key=lambda x: x[6])
    i=0
    while i in range(len(sortedSeriesList)):
        samelist = [1 for x in sortedSeriesList if x[4] == sortedSeriesList[i][4]]
        for j in range(len(samelist)-1):
            sortedSeriesList[i+j][6] = watchLaterList[sortedSeriesList[i+j][0]][6]
            sortedSeriesList[i+j+1][6] = watchLaterList[sortedSeriesList[i+j+1][0]][6]
            videoToMoveIndex = sortedwatchLaterList.index(tuple(sortedSeriesList[i+j+1]))
            sortedwatchLaterList.remove(sortedwatchLaterList[videoToMoveIndex])
            videoIndex = sortedwatchLaterList.index(tuple(sortedSeriesList[i+j]))
            if len(samelist) > 2:
                sortedwatchLaterList.insert(videoIndex+2,tuple(sortedSeriesList[i+j+1]))
            else:
                sortedwatchLaterList.insert(videoIndex+1,tuple(sortedSeriesList[i+j+1]))
        i += len(samelist)
    videoIDList = [item[2] for item in sortedwatchLaterList]
    for i in range(len(videoFollowUpList)-1):
        if videoFollowUpList[2][i] in videoFollowUpList[0]:
            predecentVideoPosition = videoIDList.index(videoFollowUpList[1][videoFollowUpList[0].index(videoFollowUpList[2][i])])
            videoFollowUpPosition = videoIDList.index(videoFollowUpList[1][i])
            videoRecord = sortedwatchLaterList.pop(videoFollowUpPosition)
            if videoFollowUpPosition < predecentVideoPosition:
                positionMovement = 2
            else:
                positionMovement = 1
            sortedwatchLaterList.insert(predecentVideoPosition+positionMovement, videoRecord)
    sortedpriorityWatchLater = []
    for i in range(len(priorityWatchLater)):
        sortedpriorityWatchLater += sorted(priorityWatchLater[i], key=lambda x: x[5])
    return sortedpriorityWatchLater + sortedwatchLaterList

def renumberWatchLater(watchLater):
    for x in range(len(watchLater)):
        watchLater[x] = (x, watchLater[x][1], watchLater[x][2], watchLater[x][3], watchLater[x][4], watchLater[x][5], watchLater[x][6])
    return watchLater

#Quota Management
def getQuotaAmount(connection, projectID):
    datePatternString = "%Y-%m-%d"
    cur = connection.cursor()
    query = "SELECT MAX(date) FROM QuotaLimit Where(projectID= "+ str(projectID) + ")"
    cur.execute(query)
    for d_ in cur:
        dbDate = d_[0].strftime(datePatternString)
    if dt.datetime.now().strftime(datePatternString) == dbDate:
        cur.execute(
            "SELECT Amount FROM QuotaLimit Where Date = ? and projectID = ?" , (dbDate, projectID)
        )
        for amount in cur:
            return amount[0], dbDate
    else:
        return 0, dbDate

def saveQuota(connection, dateString, quota, projectID):
    cur = connection.cursor()
    todayDateString = dt.datetime.now().strftime("%Y-%m-%d")
    if dateString != todayDateString:
        cur.execute(
            "INSERT INTO QuotaLimit(date, amount, projectID) VALUES (?,?,?)", (todayDateString, quota, projectID)
        )
    else:
        cur.execute(
            "UPDATE QuotaLimit SET Amount = ? WHERE Date = ? and projectID = ?", (quota, dateString, projectID)
        )
    connection.commit()