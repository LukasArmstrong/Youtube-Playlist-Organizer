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
import structlog
import logging

#==================================
#            Variables
#==================================
global gNumStrikes
global gLogger
gNumStrikes = 3
gLogger = None
#==================================
#            Logging
#==================================

def initLogger(file, debug=False, verbose=False):
    if debug:
        if verbose:
            structlog.configure( 
                processors=[ 
                    structlog.contextvars.merge_contextvars,
                    structlog.processors.TimeStamper(fmt="iso"),
                    structlog.stdlib.add_log_level,
                    structlog.processors.EventRenamer("msg"),
                    structlog.processors.CallsiteParameterAdder(
                        [structlog.processors.CallsiteParameter.FUNC_NAME,
                        structlog.processors.CallsiteParameter.LINENO,
                        structlog.processors.CallsiteParameter.PROCESS,
                        structlog.processors.CallsiteParameter.THREAD]
                    ),
                    structlog.processors.dict_tracebacks,
                    structlog.processors.JSONRenderer(),
                ],
                wrapper_class=structlog.make_filtering_bound_logger(logging.INFO), 
                context_class=dict, 
                cache_logger_on_first_use=True
            )
        else:
            structlog.configure( 
                processors=[ 
                    structlog.contextvars.merge_contextvars,
                    structlog.processors.TimeStamper(fmt="iso"),
                    structlog.stdlib.add_log_level,
                    structlog.processors.EventRenamer("msg"),
                    structlog.processors.CallsiteParameterAdder(
                        [structlog.processors.CallsiteParameter.FUNC_NAME,
                        structlog.processors.CallsiteParameter.LINENO,
                        structlog.processors.CallsiteParameter.PROCESS,
                        structlog.processors.CallsiteParameter.THREAD]
                    ),
                    structlog.processors.dict_tracebacks,
                    structlog.processors.JSONRenderer(),
                ],
                wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG), 
                context_class=dict, 
                cache_logger_on_first_use=True
            )
    else:
        BASE_DIR = os.path.dirname(file)
        os.makedirs(os.path.join(BASE_DIR, "logs"), exist_ok=True)
        LOG_FILE = f"{BASE_DIR}\\logs\\{os.path.basename(file)[:-3]}.log"

        structlog.configure( 
            processors=[ 
                structlog.contextvars.merge_contextvars,
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.stdlib.add_log_level,
                structlog.processors.EventRenamer("msg"),
                structlog.processors.CallsiteParameterAdder(
                    [structlog.processors.CallsiteParameter.FUNC_NAME,
                    structlog.processors.CallsiteParameter.LINENO,
                    structlog.processors.CallsiteParameter.PROCESS,
                    structlog.processors.CallsiteParameter.THREAD]
                ),
                structlog.processors.dict_tracebacks,
                structlog.processors.JSONRenderer(),
            ],
            wrapper_class=structlog.make_filtering_bound_logger(logging.WARNING), 
            context_class=dict, 
            logger_factory=structlog.WriteLoggerFactory(
                file=open(LOG_FILE,'a+')
            ), 
            cache_logger_on_first_use=True
        )
    global gLogger
    gLogger = structlog.get_logger()
    gLogger.info("Logger Created!")
    return gLogger

def setLogger(logger):
    logger.info("Enter...")
    global gLogger
    gLogger = logger
    gLogger.debug("Logger set!")
    gLogger.info("Leaving...")

#==================================
#           SQL/MariaDB
#==================================
def getDataBaseConnection(usr, pswd, host, port, db):
    gLogger.info("Entering...")
    gLogger.debug("Checking types...")
    checkType(usr, str)
    checkType(pswd, str)
    checkType(host, str)
    checkType(port, int)
    checkType(db, str)
    try:
        gLogger.debug("Attempting MariaDB connection...")
        conn = mariadb.connect(
            user = usr,
            password = pswd,
            host = host,
            port = port,
            database = db
        )
        gLogger.debug("Database Connection established!")
    except mariadb.Error as e:
        gLogger.error(f"Error connecting to MariaDB Platform.  Type: {type(e)} Arguements:{e}", usr=usr, pswd=pswd, host=host, port=port, db=db)
        raise mariadb.Error(e)
    gLogger.info("Leaving...")
    return conn

def getDataDB(conn, tableString, cols, optionsString=""):
    gLogger.info("Entering...")
    gLogger.debug("Checking types...")
    checkType(tableString, str)
    checkType(cols, list)
    cur = conn.cursor()
    gLogger.info("Get connection cursor obtained...")
    query = "Select " + " ,".join(cols) +" from " + tableString + " " + optionsString
    try:
        gLogger.debug("Attempting query...")
        cur.execute(query)
        gLogger.debug("Query Successful!")
    except mariadb.Error as e:
        gLogger.error(f"Error executing query {query}.  Type: {type(e)} Arguements:{e}", conn=conn, tableString=tableString, cols=cols)
        raise mariadb.Error(e)
    gLogger.info("Leaving...")
    return cur.fetchall()

def setDataDB(conn, tableString, cols_list, vals_list, optionsString=""):
    gLogger.info("Entering 'setDataDB'...")
    gLogger.info("Checking Number of Columns = Number of values to assign...")
    if len(cols_list) != len(vals_list):
        gLogger.error("Lengths of Columns and Values differ!", DB_Connection = conn, Table = tableString, Columns = cols_list, Values = vals_list)
        raise ValueError("Lengths of Columns and Values differ!")
    gLogger.info("Checking types...")
    checkType(tableString, str)
    checkType(cols_list, list)
    checkType(vals_list, list)
    checkType(optionsString, str)
    cur = conn.cursor()
    gLogger.info("Set connection cursor obtained...")
    query = f"Insert Into {tableString}{*cols_list,}"
    query = query.replace("'", "`")
    query += f" Values {*vals_list,} {optionsString}"
    try:
        gLogger.info("Attempting query...")
        cur.execute(query)
        gLogger.info("Query Successful!")
        conn.commit()
        gLogger.info("Query Committed!")
    except mariadb.Error as e:
        gLogger.error(f"Error executing query {query}.  Type: {type(e)} Arguements:{e}", DB_Connection = conn, Table = tableString, Columns = cols_list, Values = vals_list)
        raise mariadb.Error(e)
    gLogger.info(f"Data in {tableString} set!")

def updateDataDB(conn, tableString, cols_list, vals_list, optionsString=""):
    gLogger.info("Entering 'updateDataDB'...")
    gLogger.info("Checking Number of Columns = Number of values to assign...")
    if len(cols_list) != len(vals_list):
        gLogger.error("Lengths of Columns and Values differ!", DB_Connection = conn, Table = tableString, Columns = cols_list, Values = vals_list)
        raise ValueError("Lengths of Columns and Values differ!")
    gLogger.info("Checking types...")
    checkType(tableString, str)
    checkType(cols_list, list)
    checkType(vals_list, list)
    checkType(optionsString, str)
    cur = conn.cursor()
    gLogger.info("Update connection cursor obtained...")
    query = f"Update {tableString} set { ', '.join(f'`{x}` = {str(vals_list[i])}' for i, x in enumerate(cols_list)) } {optionsString}"
    try:
        gLogger.info("Attempting query...")
        cur.execute(query)
        gLogger.info("Query Successful!")
        conn.commit()
        gLogger.info("Query Committed!")
    except mariadb.Error as e:
        gLogger.error(f"Error executing query {query}.  Type: {type(e)} Arguements:{e}", DB_Connection = conn, Table = tableString, Columns = cols_list, Values = vals_list)
        raise mariadb.Error(e)
    gLogger.info(f"{tableString} updated!")

def clearTableDB(conn, tableString):
    gLogger.info("Entering 'delDataDB'...")
    gLogger.info("Checking types...")
    checkType(tableString, str)
    cur = conn.cursor()
    gLogger.info("Delete connection cursor obtained...")
    query = f"Delete From {tableString}"
    try:
        gLogger.info("Attempting query...")
        cur.execute(query)
        gLogger.info("Query Successful!")
        conn.commit()
        gLogger.info("Query Committed!")
    except mariadb.Error as e:
        gLogger.error(f"Error executing query {query}.  Type: {type(e)} Arguements:{e}", DB_Connection = conn, Table = tableString)
        raise mariadb.Error(e)
    gLogger.info(f"{tableString} cleared!")

def storeWatchLaterDB(conn, watchlater):
    gLogger.info("Entering 'storeWatchLaterDB'...")
    gLogger.info("Checking types...")
    checkType(watchlater, list)
    clearTableDB(conn, 'WatchLaterList')
    gLogger.info("WatchLaterList Cleared...")
    gLogger.info("Filling new list...")
    for video in watchlater:
        videoList = list(video)
        videoList[6] = sanitizeTitle(videoList[6])
        setDataDB(conn, 'WatchLaterList', ['position', 'playlistID', 'videoID', 'duration', 'creator', 'publishedTimeUTC', 'title'], list(video), 'ON DUPLICATE KEY UPDATE position=Value(position)')
    gLogger.info("Watch Later stored in database!")
        

#==================================
#    Qouta Controller Functions
#==================================
def getQuotaUsed(connection, projectID):
    gLogger.info("Entering 'getQuotaUsed'...")
    optionString = "Where(projectID= "+ str(projectID) + ")"
    gLogger.debug(f"Where statement: {optionString}")
    gLogger.info("Getting Latest date...")
    dbDate = getDataDB(connection, 'QuotaLimit', ['MAX(date)'], optionString)
    gLogger.info("Latest date obtained...")
    gLogger.info("Perfroming logic if date is today or not...")
    if dt.datetime.today() == dbDate:
        gLogger.info("Date is today...")
        optionString= f"Where Date = {dbDate} and projectID = {projectID}"
        gLogger.debug(f"Where statement: {optionString}")
        gLogger.info("Getting used quota...")
        amount = getDataDB(connection,'QuotaLimit', ['Amount'], optionString)
        gLogger.info("Returning used quota and date...")
        return amount, True
    else:
        gLogger.info("Date is not today...")
        gLogger.info("Reseting quota...")
        return 0, False

def setQuotaUsed(connection, inDB, quota, projectID):
    gLogger.info("Entering 'setQuotaUsed'...")
    if not inDB:
        gLogger.info("Creating new quota record...")
        setDataDB(connection, 'QuotaLimit', ['date', 'amount', 'projectID'], [dt.date.today().strftime("%Y/%m/%d"), quota, projectID])
    else:
        gLogger.info("Updating quota record...")
        optionsString = f"Where Date = {dt.date.today()} and projectID = {projectID}"
        updateDataDB(connection, 'QuotaLimit', ['Amount'], [quota], optionsString)
    gLogger.info("Quota Set!")
        
#==================================
#          Youtube API
#==================================
def getCredentials(portNumber, clientSecretFile):
    gLogger.info("Entering 'getCredentials'...")
    credentials =  None
    # token.pickle stores the user's credentials from previously successful logins
    gLogger.info("Checking if token pickle exist...")
    if os.path.exists("token.pickle"):
        gLogger.info("Loading credentials token from file...")
        with open("token.pickle", "rb") as token:
            credentials = pickle.load(token)
        gLogger.info("credentials token loaded")
    #If there is no valid credentials available, then either refresh the token or log in.
    gLogger.info("Checking if credential token is valid...")
    if not credentials or not credentials.valid:
        gLogger.info("Credential token not valid. Checking if expired...")
        if credentials and credentials.expired and credentials.refresh_token:
            gLogger.info("Credential token expired and can be refreshed...")
            gLogger.info("Refreshing access token...")
            credentials.refresh(Request())
            gLogger.info("Token refreshed...")
            gLogger.info("Saving new token...")
            saveCredentails(credentials)
        else:
            gLogger.info("Credential token expired and can _not_ be refreshed...")
            gLogger.info("Fetching new token...")
            flow = getFlowObject(clientSecretFile)
            gLogger.info("Flow server created. Running...")
            flow.run_local_server(
                port=portNumber, 
                prompt="consent", 
                authorization_prompt_message=""
            )
            gLogger.info("Obtaining credential token...")
            credentials = flow.credentials
            gLogger.info("Credential token obtained. Saving...")
            saveCredentails(credentials)
    gLogger.info("Returning credentials")
    return credentials

def getFlowObject(clientSecretFile):
    gLogger.info("Creating Flow object...")
    return InstalledAppFlow.from_client_secrets_file(
        clientSecretFile,
        scopes=["https://www.googleapis.com/auth/youtube",
                "https://www.googleapis.com/auth/youtube.force-ssl", 
                "https://www.googleapis.com/auth/youtubepartner"]
    )

def saveCredentails(credentials):
    gLogger.info("Entering 'saveCredentials'...")
    # Save credentials for the next run
    with open("token.pickle", "wb") as f:
        gLogger.info("Saving credentials for future use...")
        pickle.dump(credentials, f)
    gLogger.info("Credentails Saved!")

def getWatchLater(youtube, playlistID, nextPageBoolean):
    gLogger.info("Entering 'getWatchLater'...")
    gLogger.info("Initalizing variables...")
    nextPageToken = None
    numberRequest = 0
    watchLaterList = []
    gLogger.info("Getting List...")
    while True:
        #Watch Later isn't available through the API, so have to use playlist as pseudo watch later list
        gLogger.info("Creating youtube playlist request...")
        pl_request = youtube.playlistItems().list(
            part="contentDetails, snippet",
            playlistId=playlistID,
            maxResults = 50, #Youtube API won't allow more then 50 results per request. Per docs: https://developers.google.com/youtube/v3/docs/playlists/list
            pageToken = nextPageToken
        )
        try:
            gLogger.info("Executing youtube playlist request...")
            pl_response = pl_request.execute()
        except Exception as e:
            gLogger.error(f"Error executing youtube playlist request. Type: {type(e)} Arguements:{e}")
            raise RuntimeError(e)
        numberRequest += 1 #Tracking quota usage
        gLogger.info("Unpacking youtube playlist response...")
        videoErrorCount = 0            
        for item in pl_response["items"]:
            video = (item["snippet"]["position"], item["id"], item["contentDetails"]["videoId"]) #tuple of position in watch later, playlist ID, and video ID
            #Need more data to sort
            gLogger.info("Creating youtube video request...")
            vid_request = youtube.videos().list(
                part="contentDetails, snippet", # the snippet property contains the channelId, title, description, tags, and categoryId properties. TODO: Make use description, tags, and categoryId properties. Data Science?
                id = item["contentDetails"]["videoId"],
            )
            gLogger.info("Executing youtube video request...")
            try:
                vid_response = vid_request.execute()
            except Exception as e:
                videoErrorCount += 1
                if videoErrorCount > gNumStrikes:
                    gLogger.error(f"Error executing youtube video request. All Strikes Used. Type: {type(e)} Arguements:{e}")
                    raise RuntimeError(e) #TODO: Better handling. -Check Quota Limit -Strikes?
                else:
                    gLogger.warning(f"Unexcepted issue executing youtube video request. Strike: {videoErrorCount} Type: {type(e)} Arguements:{e}")
                    pass
            numberRequest += 1 #Tracking quota usage
            gLogger.info("Unpacking youtube video response...")
            for vid in vid_response["items"]:
                gLogger.info("Converting video duration to more useful format...")
                duration = durationString2Sec(vid["contentDetails"]["duration"])
                gLogger.info("Converting video published date to more useful format...")
                utcPublishedTime =  dateString2EpochTime(vid["snippet"]["publishedAt"])
                gLogger.info("Storing video data...")
                videoSnippet = (duration, vid["snippet"]["channelTitle"], utcPublishedTime, vid["snippet"]["title"])
            gLogger.info("Combining video tuples...")
            video = video + videoSnippet
            gLogger.info("Adding video tuple to watch later list...")
            watchLaterList.append(video)
        gLogger.info("Checking if should get next page...")
        if nextPageBoolean:
            gLogger.info("Getting next page token...")    
            nextPageToken = pl_response.get('nextPageToken')

        gLogger.info("Checking if next page token exist...")
        if not nextPageToken:
            gLogger.info("Next page token doesn't exist, breaking out of loop...")
            break
    gLogger.info("Returning watch later list and number of requests")
    return watchLaterList, numberRequest
        
def updatePlaylist(watchLater, sortedWatchLater, youtube, playlistID):
    gLogger.info("Entering...")
    gLogger.debug("Checking types...")
    checkType(watchLater, list)
    checkType(sortedWatchLater, list)
    checkType(playlistID, str)
    #checkType youtube
    #checkType gLogger
    numOperations = 0
    videoErrorCount = 0
    gLogger.debug("Initialized variables...", numOperations=numOperations, videoErrorCount=0)
    gLogger.debug("Checking length of watch later")
    if len(watchLater) != len(sortedWatchLater):
        gLogger.error("Length of watch later lists don't match!", watchLater=watchLater, sortedWatchLater=sortedWatchLater)
        raise ValueError("Lists must have the same size")
    gLogger.debug("Entering loop for watch later...")
    for x in range(len(watchLater)):
        gLogger.debug("Checking if video position on YT wach later same as sorted watch later...") 
        if watchLater[x] != sortedWatchLater[x]: #naive approach to save on quota
            gLogger.debug("Creating update request..")
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
            gLogger.debug("Attempting to execute update request...")
            try:
                update_response = update_request.execute()
                gLogger.debug("Execution Successful!")
                try:
                    gLogger.debug("incrementing num of operations...")
                    numOperations += 1
                    gLogger.debug("Moving video record to position in sorted list")
                    watchLater.insert(x, watchLater.pop(watchLater.index(sortedWatchLater[x])))
                except Exception as e:
                    gLogger.error(f"Unexpect error updating watch later list! Type: {type(e)} Arguements:{e}")
            except Exception as e:
                videoErrorCount += 1
                if videoErrorCount > gNumStrikes:
                    gLogger.error(f"Error executing youtube update request. All Strikes Used. Type: {type(e)} Arguements:{e}")
                    raise RuntimeError(e) #TODO: Better handling. -Check Quota Limit -Strikes?
                else:
                    gLogger.warning(f"Unexcepted issue executing youtube update request. Strike: {videoErrorCount} Type: {type(e)} Arguements:{e}")
                    pass
                gLogger.error(f"Couldn't update {x[6]}. Type: {type(e)} Arguements:{e}")
    gLogger.debug(f"Number of operations preformed: {numOperations}")
    gLogger.info("Leaving...")
    return numOperations, watchLater

def getVideoYT(youtube, videoID):
    gLogger.info("Enter...")
    videoErrorCount = 0
    videoDetails = []
    gLogger.debug("Initialized variables...", videoErrorCount=videoErrorCount)
    gLogger.debug("Creating YT api video request...")
    vid_request = youtube.videos().list(
        part="contentDetails, snippet", # the snippet property contains the channelId, title, description, tags, and categoryId properties. TODO: Make use description, tags, and categoryId properties. Data Science?
        id = videoID,
    )
    gLogger.info("Executing youtube video request...")
    try:
        vid_response = vid_request.execute()
    except Exception as e:
        videoErrorCount += 1
        if videoErrorCount > gNumStrikes:
            gLogger.error(f"Error executing youtube video request. All Strikes Used. Type: {type(e)} Arguements:{e}")
            raise RuntimeError(e) #TODO: Better handling. -Check Quota Limit -Strikes?
        else:
            gLogger.warning(f"Unexcepted issue executing youtube video request. Strike: {videoErrorCount} Type: {type(e)} Arguements:{e}")
            pass
    gLogger.debug("Unpacking video response. Creating Dictionary")
    vid = vid_response["items"]
    #Better way to do this. Pretty sure response is already dictionary. Just trim/order structure dict.
    videoDetails = {
        "duration" : vid["contentDetails"]["duration"],
        "creator" :  vid["snippet"]["channelTitle"], 
        "published" : vid["snippet"]["publishedAt"],
        "title" : vid["snippet"]["title"]
    }            
    gLogger.info("Leaving...")
    return videoDetails

#def insertVideo2WatchLater(conn, youtube, playlistID, videoID):
#    watchLater = getDataDB(conn, 'WatchLaterList ', ['*'])
#    videoDetails = getVideoYT(youtube,videoID)
#    watchLater.append(len(watchLater), '', videoID, durationString2Sec(videoDetails["duration"]), videoDetails["creator"], dateString2EpochTime(videoDetails["published"]), videoDetails["title"])
#    sortedWatchLater = sortWatchLater(watchLater,)
#    updatePlaylist(youtube, playlistID, watchLater, sortedWatchLater)

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

def sortWatchLater(watchLaterList, creatorDict, keywordDict, numSerKeywords, serKeywords, videoIDFollowUpList, sequentialCreators):
    #WatchLaterList structured as (position on yt, playlist id for yt, video id for yt, duration in seconds, creator, published time in unix time, video title)
    videoCountThreshold = 50 #Number of items in list to determine priority limit
    durationThreshold = 61*60 #61 minutes in seconds / Wanted to include anything that is 60 minutes + change and under. Main goal is to stop super long content from choking up the priority queue.
    #How priority is defined
    if len(watchLaterList) > videoCountThreshold:
        priorityThreshold = 0
    else:
        priorityThreshold = 1

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
    #TODO - Break up this step
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
#=========================================
#        Quality of Life
#========================================= 
def checkType(var, type):
    gLogger.info("Entering 'checkType'...")
    if not isinstance(var, type):
        gLogger.error(f"{var} not of {type}!", variable=var, type=type)
        raise TypeError(f"{var} not of type: {type}!")
    gLogger.info("Leaving 'checkType'...")

def getProjectVariables(file):
    with open(file, 'r') as f:
        projectVariables = yaml.safe_load(f)
    return tuple(projectVariables.values())

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

def sanitizeTitle(string):
    char2Remove = '",\''
    for char in char2Remove:
        string = string.replace(char,'')
    return string