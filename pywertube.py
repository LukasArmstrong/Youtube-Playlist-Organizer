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
    #Standard Pyton logging levels debug > Info > warning > error > critical
    if debug:
        if verbose:
            structlog.configure( 
                processors=[
                    structlog.processors.TimeStamper(fmt="iso"),
                    structlog.stdlib.add_log_level,
                    structlog.processors.CallsiteParameterAdder(
                        [structlog.processors.CallsiteParameter.FUNC_NAME,
                        structlog.processors.CallsiteParameter.LINENO,
                        structlog.processors.CallsiteParameter.PROCESS,
                        structlog.processors.CallsiteParameter.THREAD]
                    ),
                    structlog.contextvars.merge_contextvars,
                    structlog.processors.dict_tracebacks,
                    structlog.dev.ConsoleRenderer(),
                ],
                wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG), 
                context_class=dict, 
                cache_logger_on_first_use=True
            )
        else:
            structlog.configure( 
                processors=[ 
                    structlog.contextvars.merge_contextvars,
                    structlog.processors.TimeStamper(fmt="iso"),
                    structlog.stdlib.add_log_level,
                    structlog.processors.CallsiteParameterAdder(
                        [structlog.processors.CallsiteParameter.FUNC_NAME,
                        structlog.processors.CallsiteParameter.LINENO,
                        structlog.processors.CallsiteParameter.PROCESS,
                        structlog.processors.CallsiteParameter.THREAD]
                    ),
                    structlog.processors.dict_tracebacks,
                    structlog.dev.ConsoleRenderer(),
                ],
                wrapper_class=structlog.make_filtering_bound_logger(logging.INFO), 
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
    gLogger.debug("Logger Created!")
    return gLogger

def setLogger(logger):
    logger.debug("Enter...")
    global gLogger
    gLogger = logger
    gLogger.debug("Logger set!")
    gLogger.debug("Leaving...")

#==================================
#           SQL/MariaDB
#==================================
def getDataBaseConnection(usr, pswd, host, port, db):
    gLogger.debug("Entering...")
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
    gLogger.debug("Leaving...")
    return conn

def getDataDB(conn, tableString, cols, optionsString=""):
    gLogger.debug("Entering...")
    gLogger.debug("Checking types...")
    checkType(tableString, str)
    checkType(cols, list)
    cur = conn.cursor()
    gLogger.debug("Get connection cursor obtained...")
    query = "Select " + " ,".join(cols) +" from " + tableString + " " + optionsString
    try:
        gLogger.debug("Attempting query...")
        cur.execute(query)
        gLogger.debug("Query Successful!")
    except mariadb.Error as e:
        gLogger.error(f"Error executing query {query}.  Type: {type(e)} Arguements:{e}", conn=conn, tableString=tableString, cols=cols)
        raise mariadb.Error(e)
    gLogger.debug("Leaving...")
    return cur.fetchall()

def setDataDB(conn, tableString, cols_list, vals_list, optionsString=""):
    gLogger.debug("Entering...")
    gLogger.debug("Checking Number of Columns = Number of values to assign...")
    if len(cols_list) != len(vals_list):
        gLogger.error("Lengths of Columns and Values differ!", DB_Connection = conn, Table = tableString, Columns = cols_list, Values = vals_list)
        raise ValueError("Lengths of Columns and Values differ!")
    gLogger.debug("Checking types...")
    checkType(tableString, str)
    checkType(cols_list, list)
    checkType(vals_list, list)
    checkType(optionsString, str)
    cur = conn.cursor()
    gLogger.debug("Set connection cursor obtained!")
    query = f"Insert Into {tableString}{*cols_list,}"
    query = query.replace("'", "`")
    query += f" Values {*vals_list,} {optionsString}"
    try:
        gLogger.debug("Attempting query...")
        cur.execute(query)
        gLogger.debug("Query Successful!")
        conn.commit()
        gLogger.debug("Query Committed!")
    except mariadb.Error as e:
        gLogger.error(f"Error executing query {query}.  Type: {type(e)} Arguements:{e}", DB_Connection = conn, Table = tableString, Columns = cols_list, Values = vals_list)
        raise mariadb.Error(e)
    gLogger.debug(f"Leaving...")

def updateDataDB(conn, tableString, cols_list, vals_list, optionsString=""):
    gLogger.debug("Entering...")
    gLogger.debug("Checking Number of Columns = Number of values to assign...")
    if len(cols_list) != len(vals_list):
        gLogger.error("Lengths of Columns and Values differ!", DB_Connection = conn, Table = tableString, Columns = cols_list, Values = vals_list)
        raise ValueError("Lengths of Columns and Values differ!")
    gLogger.debug("Checking types...")
    checkType(tableString, str)
    checkType(cols_list, list)
    checkType(vals_list, list)
    checkType(optionsString, str)
    cur = conn.cursor()
    gLogger.debug("Update connection cursor obtained!")
    query = f"Update {tableString} set { ', '.join(f'`{x}` = {str(vals_list[i])}' for i, x in enumerate(cols_list)) } {optionsString}"
    try:
        gLogger.debug("Attempting query...")
        cur.execute(query)
        gLogger.debug("Query Successful!")
        conn.commit()
        gLogger.debug("Query Committed!")
    except mariadb.Error as e:
        gLogger.error(f"Error executing query {query}.  Type: {type(e)} Arguements:{e}", DB_Connection = conn, Table = tableString, Columns = cols_list, Values = vals_list)
        raise mariadb.Error(e)
    gLogger.debug(f"Leaving...")

def clearTableDB(conn, tableString):
    gLogger.debug("Entering...")
    gLogger.debug("Checking types...")
    checkType(tableString, str)
    cur = conn.cursor()
    gLogger.debug("Delete connection cursor obtained!")
    query = f"Delete From {tableString}"
    try:
        gLogger.debug("Attempting query...")
        cur.execute(query)
        gLogger.debug("Query Successful!")
        conn.commit()
        gLogger.debug("Query Committed!")
    except mariadb.Error as e:
        gLogger.error(f"Error executing query {query}.  Type: {type(e)} Arguements:{e}", DB_Connection = conn, Table = tableString)
        raise mariadb.Error(e)
    gLogger.debug(f"Leaving...")

def storeWatchLaterDB(conn, watchlater):
    gLogger.debug("Entering...")
    gLogger.debug("Checking types...")
    checkType(watchlater, list)
    clearTableDB(conn, 'WatchLaterList')
    gLogger.debug("WatchLaterList Cleared!")
    gLogger.debug("Filling new list...")
    for video in watchlater:
        videoList = list(video)
        videoList[6] = sanitizeTitle(videoList[6])
        setDataDB(conn, 'WatchLaterList', ['position', 'playlistID', 'videoID', 'duration', 'creator', 'publishedTimeUTC', 'title'], list(video), 'ON DUPLICATE KEY UPDATE position=Value(position)')
    gLogger.debug("Watch Later stored in database!")
    gLogger.debug(f"Leaving...")
        

#==================================
#    Qouta Controller Functions
#==================================
def getQuotaUsed(connection, projectID):
    gLogger.debug("Entering...")
    optionString = "Where(projectID= "+ str(projectID) + ")"
    gLogger.debug(f"Where statement: {optionString}")
    gLogger.debug("Getting Latest date...")
    dbDate = getDataDB(connection, 'QuotaLimit', ['MAX(date)'], optionString)
    gLogger.debug("Latest date obtained!")
    gLogger.debug("Perfroming logic if date is today or not...")
    if dt.datetime.today() == dbDate:
        gLogger.debug("Date is today...")
        optionString= f"Where Date = {dbDate} and projectID = {projectID}"
        gLogger.debug(f"Where statement: {optionString}")
        gLogger.debug("Getting used quota...")
        amount = getDataDB(connection,'QuotaLimit', ['Amount'], optionString)
        gLogger.debug("Returning used quota and date...")
        return amount, True
    else:
        gLogger.debug("Date is not today...")
        gLogger.debug("Reseting quota...")
        return 0, False

def setQuotaUsed(connection, inDB, quota, projectID):
    gLogger.debug("Entering...")
    if not inDB:
        gLogger.debug("Creating new quota record...")
        setDataDB(connection, 'QuotaLimit', ['date', 'amount', 'projectID'], [dt.date.today().strftime("%Y/%m/%d"), quota, projectID])
    else:
        gLogger.debug("Updating quota record...")
        optionsString = f"Where Date = {dt.date.today()} and projectID = {projectID}"
        updateDataDB(connection, 'QuotaLimit', ['Amount'], [quota], optionsString)
    gLogger.debug("Quota Set!")
    gLogger.debug("Leaving...")
        
#==================================
#          Youtube API
#==================================
def getCredentials(portNumber, clientSecretFile):
    gLogger.debug("Entering...")
    credentials =  None
    # token.pickle stores the user's credentials from previously successful logins
    gLogger.debug("Checking if token pickle exist...")
    if os.path.exists("token.pickle"):
        gLogger.debug("Loading credentials token from file...")
        with open("token.pickle", "rb") as token:
            credentials = pickle.load(token)
        gLogger.debug("credentials token loaded")
    #If there is no valid credentials available, then either refresh the token or log in.
    gLogger.debug("Checking if credential token is valid...")
    if not credentials or not credentials.valid:
        gLogger.debug("Credential token not valid. Checking if expired...")
        if credentials and credentials.expired and credentials.refresh_token:
            gLogger.debug("Credential token expired and can be refreshed...")
            gLogger.debug("Refreshing access token...")
            credentials.refresh(Request())
            gLogger.debug("Token refreshed!")
            saveCredentails(credentials)
        else:
            gLogger.debug("Credential token expired and can _not_ be refreshed...")
            gLogger.debug("Fetching new token...")
            flow = getFlowObject(clientSecretFile)
            gLogger.debug("Flow server created. Running...")
            flow.run_local_server(
                port=portNumber, 
                prompt="consent", 
                authorization_prompt_message=""
            )
            gLogger.debug("Obtaining credential token...")
            credentials = flow.credentials
            gLogger.debug("Credential token obtained!")
            saveCredentails(credentials)
    gLogger.debug("Returning credentials...")
    return credentials

def getFlowObject(clientSecretFile):
    gLogger.debug("Enter...")
    gLogger.debug("Creating Flow object...")
    return InstalledAppFlow.from_client_secrets_file(
        clientSecretFile,
        scopes=["https://www.googleapis.com/auth/youtube",
                "https://www.googleapis.com/auth/youtube.force-ssl", 
                "https://www.googleapis.com/auth/youtubepartner"]
    )

def saveCredentails(credentials):
    gLogger.debug("Entering...")
    # Save credentials for the next run
    with open("token.pickle", "wb") as f:
        gLogger.debug("Saving credentials for future use...")
        pickle.dump(credentials, f)
    gLogger.debug("Credentails Saved!")
    gLogger.debug("Leaving...")

def getWatchLater(youtube, playlistID, nextPageBoolean):
    gLogger.debug("Entering...")
    gLogger.debug("Initalizing variables...")
    nextPageToken = None
    numberRequest = 0
    watchLaterList = []
    gLogger.debug("Getting List...")
    while True:
        #Watch Later isn't available through the API, so have to use playlist as pseudo watch later list
        gLogger.debug("Creating youtube playlist request...")
        pl_request = youtube.playlistItems().list(
            part="contentDetails, snippet",
            playlistId=playlistID,
            maxResults = 50, #Youtube API won't allow more then 50 results per request. Per docs: https://developers.google.com/youtube/v3/docs/playlists/list
            pageToken = nextPageToken
        )
        try:
            gLogger.debug("Executing youtube playlist request...")
            pl_response = pl_request.execute()
            gLogger.debug("Playlist request executed!")
        except Exception as e:
            gLogger.error(f"Error executing youtube playlist request. Type: {type(e)} Arguements:{e}")
            raise RuntimeError(e)
        numberRequest += 1 #Tracking quota usage
        gLogger.debug("Unpacking youtube playlist response...")
        videoErrorCount = 0            
        for item in pl_response["items"]:
            video = (item["snippet"]["position"], item["id"], item["contentDetails"]["videoId"]) #tuple of position in watch later, playlist ID, and video ID
            #Need more data to sort
            gLogger.debug("Creating youtube video request...")
            vid_request = youtube.videos().list(
                part="contentDetails, snippet", # the snippet property contains the channelId, title, description, tags, and categoryId properties. TODO: Make use description, tags, and categoryId properties. Data Science?
                id = item["contentDetails"]["videoId"],
            )
            gLogger.debug("Executing youtube video request...")
            try:
                vid_response = vid_request.execute()
                gLogger.debug("Video request executed!")
            except Exception as e:
                videoErrorCount += 1
                if videoErrorCount > gNumStrikes:
                    gLogger.error(f"Error executing youtube video request. All Strikes Used. Type: {type(e)} Arguements:{e}")
                    raise RuntimeError(e) #TODO: Better handling. -Check Quota Limit -Strikes?
                else:
                    gLogger.warning(f"Unexcepted issue executing youtube video request. Strike: {videoErrorCount} Type: {type(e)} Arguements:{e}")
                    pass
            numberRequest += 1 #Tracking quota usage
            gLogger.debug("Unpacking youtube video response...")
            for vid in vid_response["items"]:
                gLogger.debug("Converting video duration to more useful format...")
                duration = durationString2Sec(vid["contentDetails"]["duration"])
                gLogger.debug("Converting video published date to more useful format...")
                utcPublishedTime =  dateString2EpochTime(vid["snippet"]["publishedAt"])
                gLogger.debug("Video duration and publish time converted! Storing in tuple...")
                videoSnippet = (duration, vid["snippet"]["channelTitle"], utcPublishedTime, vid["snippet"]["title"])
            gLogger.debug("Combining video tuples...")
            video = video + videoSnippet
            gLogger.debug("Adding video tuple to watch later list...")
            watchLaterList.append(video)
        gLogger.debug("Checking if should get next page...")
        if nextPageBoolean:
            gLogger.debug("Getting next page token...")    
            nextPageToken = pl_response.get('nextPageToken')

        gLogger.debug("Checking if next page token exist...")
        if not nextPageToken:
            gLogger.debug("Next page token doesn't exist, breaking out of loop...")
            break
    gLogger.debug("Returning watch later list and number of requests...")
    return watchLaterList, numberRequest
        
def updatePlaylist(watchLater, sortedWatchLater, youtube, playlistID):
    gLogger.debug("Entering...")
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
    gLogger.debug("Leaving...")
    return numOperations, watchLater

def getVideoYT(youtube, videoID):
    gLogger.debug("Enter...")
    videoErrorCount = 0
    videoDetails = []
    gLogger.debug("Initialized variables...", videoErrorCount=videoErrorCount)
    gLogger.debug("Creating YT api video request...")
    vid_request = youtube.videos().list(
        part="contentDetails, snippet", # the snippet property contains the channelId, title, description, tags, and categoryId properties. TODO: Make use description, tags, and categoryId properties. Data Science?
        id = videoID,
    )
    gLogger.debug("Attempting youtube video request...")
    try:
        vid_response = vid_request.execute()
        gLogger.debug("Youtube Video Request executed successfully!")
    except Exception as e:
        videoErrorCount += 1
        if videoErrorCount > gNumStrikes:
            gLogger.error(f"Error executing youtube video request. All Strikes Used. Type: {type(e)} Arguements:{e}")
            raise RuntimeError(e) #TODO: Better handling. -Check Quota Limit -Strikes?
        else:
            gLogger.warning(f"Unexcepted issue executing youtube video request. Strike: {videoErrorCount} Type: {type(e)} Arguements:{e}")
            pass
    gLogger.debug("Unpacking video response... Creating Dictionary...")
    vid = vid_response["items"]
    #Better way to do this. Pretty sure response is already dictionary. Just trim/order structure dict.
    videoDetails = {
        "duration" : vid["contentDetails"]["duration"],
        "creator" :  vid["snippet"]["channelTitle"], 
        "published" : vid["snippet"]["publishedAt"],
        "title" : vid["snippet"]["title"],
        "description" : vid["snippet"]["description"],
        "tags" : vid["snippet"]["tag"],
        "categoryIDs": vid["snippet"]["categoryId properties"]
    }            
    gLogger.debug("Video Dictonary Created!")
    gLogger.debug("Leaving...")
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
    gLogger.debug("Entering...")
    nonPriority = watchLaterList.copy() #creates copy to return non priority videos as well
    gLogger.debug("Watch Later List copied!")
    creatorDict = filterDict(creatorDict, ">", priorityThreshold) #Filter dictionary for creators not considered priority. Priority changes with size of watchlater
    keywordDict = filterDict(keywordDict, ">", priorityThreshold) #Filter dictionary for Keywords not considered priority. Priority changes with size of watchlater
    gLogger.debug("Creator and Keyboard dictionaries filtered!")
    gLogger.debug("Making set from Creator dictionary values...", creatorDictValues=creatorDict.values())
    setValues = set(list(creatorDict.values())) #remove duplicate priortity scores. Multiple creators are on the same tier of priority.
    gLogger.debug("Adding Keyphrase dictionary values to set...", KeyphraseDictValues=keywordDict.values())
    setValues.update(list(keywordDict.values())) #add any unique priority score from keyphrase
    gLogger.debug("Converting set to list and reverse sorting...")
    scoreSet = sorted(list(setValues), reverse=True) #sort priority scores from high to low
    gLogger.debug("Priority Scores list created!")
    gLogger.debug(f"Creating {len(scoreSet)} dimensional list (Size is determined by number of priority scores)...")
    priorityWatchLater = [[] for i in range(len(scoreSet))]  #create 2d array where each row is a different creator priorirty score"
    gLogger.debug("Looping over watch later list to find priority videos...")
    for item in watchLaterList:
        #Check Keyword first because some creators have natural priority, but subset of videos from said creator have higher priority 
        keywordFound = False #init skip flag
        for word in keywordDict.keys():
            if word in item[6]:
                if keywordDict[word] in scoreSet:
                    gLogger.debug(f"Found Video, {item[6]}, is priority with keyword {word}!")
                    gLogger.debug("Finding priority score index of video...")
                    scoreIndex = scoreSet.index(keywordDict[word]) #find priority row index
                    gLogger.debug("Appending priority video to priority list...")
                    priorityWatchLater[scoreIndex].append(item) #add video to priority row
                    gLogger.debug("Removing priority video from orginal list...")
                    nonPriority.remove(item) #remove priority video from non-priority list
                    keywordFound = True #set skip flag
                    gLogger.debug(f"{item[6]} added to priority list and removed from original list due to keyword {word}!")
                    break
        if keywordFound:
            gLogger.debug("Since Keyword found, skipping creator check...")
            continue #if keyphrase is found in item, don't care if creator of video is also priority. Skip over last section
        if item[4] in creatorDict.keys():
            if item[3]<(durationThreshold): #duration limit important because some priority creators live stream and release VOD later. Not currently interested in VOD
                if creatorDict[item[4]] in scoreSet:
                    gLogger.debug(f"Found Video, {item[6]}, is priority with creator {item[4]}!")
                    gLogger.debug("Finding priority score index of video...")
                    scoreIndex = scoreSet.index(creatorDict[item[4]]) #find priority row index
                    gLogger.debug("Appending priority video to priority list...")
                    priorityWatchLater[scoreIndex].append(item) #add videot o priority row
                    gLogger.debug("Removing priority video from orginal list...")
                    nonPriority.remove(item) #remove priority video from non-priority list
                    gLogger.debug(f"{item[6]} added to priority list and removed from original list due to creator {item[4]}!")
    gLogger.debug("Returning to priority watch later and non-priority list")
    return priorityWatchLater, nonPriority

def getSerializedVideos(watchLaterList, numSerKeywords, serKeywords):
    #Explaination: Pull out a video that is apart of a series. Videos apart of a series, store in list that all from same creator. 
    #       Typically creators are more likely to name the episodes in a series similarly. Making it easier on natural sort to do it's job later.
    gLogger.debug("Entering...")
    nonSerialized = watchLaterList.copy() #creates copy to return non serialized videos as well
    gLogger.debug("Watch Later List copied!")
    seriesPattern = re.compile(r"(%s)\s\d+" % "|".join(numSerKeywords) + "|(%s)" % "|".join(serKeywords), re.IGNORECASE) #create complicated regex pattern to find serialized videos through keywords
    gLogger.debug("Regular Expression created to find Serialized Videos")
    gLogger.debug("Creating creator list...")
    creators = [i[4] for i in watchLaterList] #pull out creators from given list
    gLogger.debug("Removing dupes from creator list...")
    creatorsSet = list(set(creators)) #remove dups
    gLogger.debug("Creator List Created!")
    gLogger.debug(f"Creating {len(creatorsSet)} dimensional list (Size is determined by number of creators in watch later)...")
    seriesList = [[] for i in range(len(creatorsSet))] #create 2d array where each row is a different creator
    gLogger.debug("Looping over watch later list to find serialized videos...")
    for item in watchLaterList:
        result = seriesPattern.search(item[6]) #check if pattern in video title
        if result:
            gLogger.debug(f"Result found! {result} in {item[6]}!")
            gLogger.debug("Finding creator index of video...")
            scoreIndex = creatorsSet.index(item[4]) #postion in list determines row number
            gLogger.debug("Appending serialized video to creator sub-list...")
            seriesList[scoreIndex].append(item) #place corresponding videos to creator row
            gLogger.debug("Removing serialized video from orginal list...")
            nonSerialized.remove(item) #remove serialized video from non-serialized list
    gLogger.debug("Returning to serialized watch later and non-serialized list")
    return seriesList, nonSerialized

def getSequentialVideos(watchLaterList, sequentialCreators,durationThreshold):
    #TODO: Instead of durationThreshold as filter, use blacklist. 
    #Explaination: Pull out videos from creators that reference previous videos. Since it just the order the creator uploaded them, published date can be used to sort.
    gLogger.debug("Entering...")
    nonSequential = watchLaterList.copy() #creates copy to return non sequential videos as well
    gLogger.debug("Watch Later List copied!")
    gLogger.debug(f"Creating {len(sequentialCreators)} dimensional list (Size is determined by number of creators in sequential)...")
    seqList = [[] for i in range(len(sequentialCreators))] #create 2d array where each row is a different creator
    gLogger.debug("Looping over watch later list to find serialized videos...")
    for video in watchLaterList:
        if video[4] in sequentialCreators and video[3] < durationThreshold:
            gLogger.debug(f"Result found! {video[6]} by {video[4]}!")
            gLogger.debug("Finding creator index of video...")
            seqIndex = sequentialCreators.index(video[4]) #postion in list determines row number
            gLogger.debug("Appending sequential video to creator sub-list...")
            seqList[seqIndex].append(video) #place corresponding videos to creator row
            gLogger.debug("Removing serialized video from orginal list...")
            nonSequential.remove(video) #remove serialized video from non-serialized list
    gLogger.debug("Returning to serialized watch later and non-serialized list")
    return seqList, nonSequential

def getFollowUpVideos(watchLaterList, FollowUpIDList):
    gLogger.debug("Entering...")
    gLogger.debug("Pulling out videos records from watcherlater if in Follow up list...")
    videos = [item for item in watchLaterList if item[2] in FollowUpIDList[1]] #get all videos records that match on in follow up
    gLogger.debug("Pulling out parent videos from follow up list...")
    ids = [v for i, v in enumerate(FollowUpIDList[0]) if FollowUpIDList[2][i] is None] #find all parent videos
    nullCount = FollowUpIDList[2].count(None) #get the number of parent videos
    FollowUpWatchLater = [[] for i in range(nullCount)] #create 2d array where each row is collection of follow up videos
    gLogger.debug("Entering loop to order follow up...")
    for index, id in enumerate(FollowUpIDList[2]):
        if id is None: #If parent ID
            gLogger.debug("Getting ID of Parent video...")
            idIndex = ids.index(FollowUpIDList[0][index])
        else:
            gLogger.debug("Getting ID of Child video...")
            idIndex = ids.index(id)
        gLogger.debug("Ordering follow up video...")
        FollowUpWatchLater[idIndex].append(videos[index])
    gLogger.debug("Returning follow up watch later...")
    return FollowUpWatchLater

def sortSeriesVideos(watchLaterList):
    gLogger.debug("Entering...")
    gLogger.debug("Looping over series list...")
    for index in range(len(watchLaterList)):
        gLogger.debug("Natural sorting series sub list...")
        watchLaterList[index] = natsorted(watchLaterList[index], key=lambda x: x[6]) #since each creator has own method for ordering videos, natsort each creator individually
    gLogger.debug("Returning sorted series list...")
    return watchLaterList

def sortSequentialVideo(watchLaterList):
    gLogger.debug("Entering...")
    gLogger.debug("Looping over sequential list...")
    for index in range(len(watchLaterList)):
        gLogger.debug("Sorting sequential sub list...")
        watchLaterList[index] = sorted(watchLaterList[index], key=lambda x: x[5]) #since each creator has own method for ordering videos, natsort each creator individually
    gLogger.debug("Returning sorted sequential list...")
    return watchLaterList

def sortWatchLater(watchLaterList, creatorDict, keywordDict, numSerKeywords, serKeywords, videoIDFollowUpList, sequentialCreators):
    #WatchLaterList structured as (position on yt, playlist id for yt, video id for yt, duration in seconds, creator, published time in unix time, video title)
    gLogger.debug("Entering...")
    gLogger.debug("Initalizing variables...")
    videoCountThreshold = 50 #Number of items in list to determine priority limit
    durationThreshold = 61*60 #61 minutes in seconds / Wanted to include anything that is 60 minutes + change and under. Main goal is to stop super long content from choking up the priority queue.
    sortedpriorityWatchLater = [] 
    
    gLogger.debug("Determining priority threshold")
    #How priority is defined
    if len(watchLaterList) > videoCountThreshold:
        priorityThreshold = 0
        gLogger.debug("Watch later is long! No threshold set")
    else:
        priorityThreshold = 1
        gLogger.debug("Watch later is short! Threshold set")

    #Step 1 - Get sublists
    gLogger.debug("Getting sub list (Priority, Follow-up, Series, and Sequential)...")
    gLogger.debug("Getting Priority list...")
    priorityWatchLater, workingWatchLater = getPriorityVideos(watchLaterList, creatorDict, keywordDict, priorityThreshold, durationThreshold)
    gLogger.debug("Priority list obtained! Getting Follow-up list...")
    followUpWatchLater = getFollowUpVideos(workingWatchLater, videoIDFollowUpList)
    gLogger.debug("Follow-up list obtained! Getting Serialized list...")
    seriesWatchLater, workingWatchLater = getSerializedVideos(workingWatchLater, numSerKeywords, serKeywords)
    gLogger.debug("Serialized list obtained! Getting Sequential list...")
    sequentialWatchLater, workingWatchLater = getSequentialVideos(workingWatchLater, sequentialCreators, durationThreshold)
    gLogger.debug("Sequential list obtained! Moving to sorting...")

    #Step 2 - Sort segments
    gLogger.debug("Sorting Priority list...")
    for i in range(len(priorityWatchLater)): #Sort by priortity then publish time
        sortedpriorityWatchLater += sorted(priorityWatchLater[i], key=lambda x: x[5]) #Creates 1D list where priority is maintaied and videos are sorted by publish time within a priority group
    gLogger.debug("Priority list sorted! Sorting Series list...")
    sortedSeriesWatchLater = sortSeriesVideos(seriesWatchLater)
    gLogger.debug("Series list sorted! Sorting Sequential list...")
    sortedSequentialWatchLater = sortSequentialVideo(sequentialWatchLater)
    gLogger.debug("Sequential list sorted! Sorting rest of watch later list...")
    workingWatchLater.sort( key=lambda x: (x[3], x[5])) #Sort by duration then publish time
    gLogger.debug("Rest of Watch Later list sorted!")

    #Step 3 - Merge sequential and series segments back together
    #TODO - Break up this step
    gLogger.debug("Entering loop to merging Series and Sequential lists together...")
    for index, item in enumerate(workingWatchLater):
        #Merge in sequential videos
        gLogger.debug("Looping over Sequential list...")
        for row in range(len(sortedSequentialWatchLater)):
            gLogger.debug("Checking if parent video duration is less than current item duration and they they are not same...")
            if sortedSequentialWatchLater[row] and sortedSequentialWatchLater[row][0][3] <= item[3] and sortedSequentialWatchLater[row][0][4] != item[4]:
                gLogger.debug("It is! Reordering video...")
                workingWatchLater.insert(index, sortedSequentialWatchLater[row][0])
                gLogger.debug("Removing parent from Sequential list...")
                sortedSequentialWatchLater[row].pop(0)

        gLogger.debug("Looping over Series list...")        
        for row in range(len(sortedSeriesWatchLater)):
            gLogger.debug("Checking if series video duration is less than current item duration and they they are not same...")
            if sortedSeriesWatchLater[row] and sortedSeriesWatchLater[row][0][3] <= item[3] and sortedSeriesWatchLater[row][0][4] != item[4]:
                gLogger.debug("It is! Reordering video...")
                workingWatchLater.insert(index, sortedSeriesWatchLater[row][0])
                gLogger.debug("Removing parent from Sequential list...")
                sortedSeriesWatchLater[row].pop(0)

    #Step 4 - Reorder follow up videos
    gLogger.debug("Entering loop to reorder follow-up videos in watch later list...")
    for row in followUpWatchLater:
        gLogger.debug("Determining position movement...")
        if len(row)>3:
            positionMovement = 2
        else:
            positionMovement = 1
        gLogger.debug("Looping over each follow-up collection...")
        for i in range(len(row)-1):
            gLogger.debug(f"Getting index of {i} video in collection...")
            predecentVideoPosition = workingWatchLater.index(row[i])
            gLogger.debug(f"Removing {i+1} video in collection from watch later list...")            
            workingWatchLater.remove(row[i+1])
            gLogger.debug(f"Inserting {i+1} video in collection...")
            workingWatchLater.insert(predecentVideoPosition+positionMovement, row[i+1])

    #Step 5 - Combine priority and non-priority watch later
    gLogger.debug("returning combination of sorted Priority and working watch later")
    return sortedpriorityWatchLater + workingWatchLater

#=========================================
#        Quality of Life
#========================================= 
def checkType(var, type):
    gLogger.debug("Entering 'checkType'...")
    if not isinstance(var, type):
        gLogger.error(f"{var} not of {type}!", variable=var, type=type)
        raise TypeError(f"{var} not of type: {type}!")
    gLogger.debug("Leaving 'checkType'...")

def renumberWatchLater(watchLater):
    for x in range(len(watchLater)):
        watchLater[x] = (x, watchLater[x][1], watchLater[x][2], watchLater[x][3], watchLater[x][4], watchLater[x][5], watchLater[x][6])
    return watchLater

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