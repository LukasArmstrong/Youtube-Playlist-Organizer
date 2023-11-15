#Name comes from PYthon poWER user code for youTUBE
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
            try:
                update_response = update_request.execute()
            except ValueError:
                print(f"Couldn't update {x[6]}. Please look into further")
            numOperations += 1
            watchLater.insert(x, watchLater.pop(watchLater.index(sortedWatchLater[x])))
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

#=========================================
#         SORTING WATCHLATER
#=========================================   
def getPriorityVideos(watchLaterList, creatorDict, keywordDict, priorityThreshold, durationThreshold):
    nonPriority = watchLaterList.copy() #creates copy to return non priority videos as well
    creatorDict = filterDict(creatorDict, ">", priorityThreshold) #Filter dictionary for creators not considered priority. Priority changes with size of watchlater
    keywordDict = filterDict(keywordDict, ">", priorityThreshold) #Filter dictionary for Keywords not considered priority. Priority changes with size of watchlater
    setValues = set(list(creatorDict.values())) #remove duplicate priortity scores. Multiple creators are on the same tier of priority.
    setValues.update(list(keywordDict.values())) #add any unique priority score from keyphrase
    scoreSet = sorted(list(setValues), reverse=True) #sort priority scores from high to low
    priorityWatchLater = [[] for i in range(len(scoreSet))]  #create 2d array where each row is a different creator priorirty score
    for item in watchLaterList:
        keywordFound = False #init skip flag
        for word in keywordDict.keys():
            if word in item[6]:
                if keywordDict[word] in scoreSet:
                    scoreIndex = scoreSet.index(keywordDict[word]) #find priority row index
                    priorityWatchLater[scoreIndex].append(item) #add video to priority row
                    nonPriority.remove(item) #remove priority video from non-priority list
                    keywordFound = True #set skip flag
                    break
        if keywordFound:
            continue #if keyphrase is found in item, don't care if creator of video is also priority. Skip over last section
        if item[4] in creatorDict.keys():
            if item[3]<(durationThreshold): #duration limit important because some priority creators live stream and release VOD later. Not currently interested in VOD
                if creatorDict[item[4]] in scoreSet:
                    scoreIndex = scoreSet.index(creatorDict[item[4]]) #find priority row index
                    priorityWatchLater[scoreIndex].append(item) #add videot o priority row
                    nonPriority.remove(item) #remove priority video from non-priority list
    return priorityWatchLater, nonPriority

def getSerializedVideos(watchLaterList, numSerKeywords, serKeywords):
    nonSerialized = watchLaterList.copy() #creates copy to return non serialized videos as well
    seriesPattern = re.compile(r"(%s)\s\d+" % "|".join(numSerKeywords) + "|(%s)" % "|".join(serKeywords), re.IGNORECASE) #create complicated regex pattern to find serialized videos through keywords
    creators = [i[4] for i in watchLaterList] #pull out creators from given list
    creatorsSet = list(set(creators)) #remove dups
    seriesList = [[] for i in range(len(creatorsSet))] #create 2d array where each row is a different creator
    for item in watchLaterList:
        result = seriesPattern.search(item[6]) #check if pattern in video title
        if result:
            scoreIndex = creatorsSet.index(item[4]) #postion in list determines row number
            seriesList[scoreIndex].append(item) #place corresponding videos to creator row
            nonSerialized.remove(item) #remove serialized video from non-serialized list
    return seriesList, nonSerialized

def getSequentialVideos(watchLaterList, sequentialCreators):
    return [i for i in watchLaterList if i[4] in sequentialCreators]

def getFollowUpVideos(watchLaterList, FollowUpIDList):
    videos = [item for item in watchLaterList if item[2] in FollowUpIDList[1]]
    ids = [v for i, v in enumerate(FollowUpIDList[0]) if FollowUpIDList[2][i] is None]
    nullCount = FollowUpIDList[2].count(None)
    FollowUpWatchLater = [[] for i in range(nullCount)]
    for index, id in enumerate(FollowUpIDList[2]):
        if id is None: 
            idIndex = ids.index(FollowUpIDList[0][index])
        else:
            idIndex = ids.index(id)
        FollowUpWatchLater[idIndex].append(videos[index])
    return FollowUpWatchLater

def sortSeriesVideos(watchLaterList):
    for row in watchLaterList:
        row = natsorted(row, key=lambda x: x[6]) #since each creator has own method for ordering videos, natsort each creator individually
    return watchLaterList

def sortWatcherLater(watchLaterList, creatorDict, keywordDict, numSerKeywords, serKeywords, videoIDFollowUpList):
    #WatchLaterList structured as (position on yt, playlist id for yt, video id for yt, duration in seconds, creator, published time in unix time, video title)
    videoCountThreshold = 50 #Number of items in list to determine priority limit
    durationThreshold = 41*60 #41 minutes in seconds / Wanted to include anything that is 40 minutes + change and under. Main goal is to stop super long content from choking up the priority queue.
    #How priority is defined
    if len(watchLaterList) > videoCountThreshold:
        priorityThreshold = 0
    else:
        priorityThreshold = 1
    sequentialCreators = ['Wintergatan']

    #Step 1 - Get sublists
    priorityWatchLater, workingWatchLater = getPriorityVideos(watchLaterList, creatorDict, keywordDict, priorityThreshold, durationThreshold)
    followUpWatchLater = getFollowUpVideos(workingWatchLater, videoIDFollowUpList)
    seriesWatchLater, workingWatchLater = getSerializedVideos(workingWatchLater, numSerKeywords, serKeywords)
    sequentialWatchLater = getSequentialVideos(workingWatchLater, sequentialCreators)
    workingWatchLater = [i for i in workingWatchLater if i not in sequentialWatchLater]
    
    #Step 2 - Sort segments
    sortedpriorityWatchLater = []
    for i in range(len(priorityWatchLater)): #Sort by priortity then publish time
        sortedpriorityWatchLater += sorted(priorityWatchLater[i], key=lambda x: x[5]) #Creates 1D list where priority is maintaied and videos are sorted by publish time within a priority group
    sortedSeriesWatchLater = sortSeriesVideos(seriesWatchLater)
    sequentialWatchLater.sort(key=lambda x: x[5]) #Sort by publish time
    workingWatchLater.sort( key=lambda x: (x[3], x[5])) #Sort by duration then publish time

    #Step 3 - Merge sequential and series segments back together
    for index, item in enumerate(workingWatchLater):
        #Merge in sequential videos
        if sequentialWatchLater and (sequentialWatchLater[0][3]  <= item[3]):
            workingWatchLater.insert(index, sequentialWatchLater[0])
            sequentialWatchLater.pop(0)
        #Merge in series videos
        for row in range(len(sortedSeriesWatchLater)):
            if sortedSeriesWatchLater[row] and sortedSeriesWatchLater[row][0][3] <= item[3] and sortedSeriesWatchLater[row][0][3] != item[4]:
                workingWatchLater.insert(index, sortedSeriesWatchLater[row][0])
                sortedSeriesWatchLater[row].pop(0)

    #Step 4 - Reorder follow up videos
    for row in followUpWatchLater:
        for i in range(len(row)-1):
            predecentVideoPosition = workingWatchLater.index(row[i])
            if len(row)>3:
                positionMovement = 2
            else:
                positionMovement = 1
            workingWatchLater.remove(row[i+1])
            workingWatchLater.insert(predecentVideoPosition+positionMovement, row[i+1])

    #Step 5 - Combine priority and non-priority watch later
    return sortedpriorityWatchLater + workingWatchLater

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