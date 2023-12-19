from flask import Flask, request, abort
import xmltodict as x2d
import pywertube as pt
import uuid
import mariadb
import os
from datetime import datetime as dt, timedelta
from googleapiclient.discovery import build

app = Flask(__name__)
if 'TERM_PROGRAM' in os.environ.keys() and os.environ['TERM_PROGRAM'] == 'vscode':
    logger = pt.initLogger(__file__, debug=True, verbose=False)
storedCreators = None

@app.route('/webhook', methods=['POST', 'GET'])
def webhook():
    if request.method == 'POST':
        data = x2d.parse(request.data)
        entry = data["feed"]["entry"]

        if dt.today().strftime("%Y-%m-%d") == entry["published"].split("T")[0]:
            if entry["author"]["name"] not in storedCreators:
                pt.insertCreator(entry["author"]["name"], entry["yt:channelId"])
                storedCreators.append(entry["author"]["name"])
            pt.insertTime(storedCreators.index(entry["author"]["name"])+1, entry["published"], entry["yt:videoId"])
        else:
            print(entry["author"]["name"] + " has updated a video!")
        return request.data
    else:
        return request.args.get('hub.challenge')

@app.route('/sort', methods=['GET'])
def sort():
    #pt.structlog.threadlocal.clear_threadlocal()
    #pt.structlog.threadlocal.bind_threadlocal(
    #    view=request.path,
    #    request_id=str(uuid.uuid4()),
    #    peer=request.access_route[0],
    #)
    msg = ""
    sortLog = logger.bind()
    pt.setLogger(sortLog)
    sortLog.info("sortLogger set as logger!")
    if request.method == 'GET':
        sortLog.info("Entering GET Request")
        try:
            pt.getDataBaseConnection(user, password, serverIp, mariaPort, database)
            sortLog.info("DataBase Connection made!")
            Data = pt.getDataDB("Creators", ["creators", "priorityScore"])
            sortLog.info("Creators and their Priority Score obtained!")
            creatorDictionary = dict(Data)
            sortLog.debug("Dictionary made from creator and priority score")
            Data = pt.getDataDB("Keyphrases", ["phrase", "score"])
            sortLog.info("Keyphrase and their Priority Score obtained!")
            keywordDictionary = dict(Data)
            sortLog.debug("Dictionary made from keyphrase and priority score")
            Data = pt.getDataDB("OrderVideos", ["id", "videoID", "predecentVideoID"])
            sortLog.info("Follow up video data obtained!")
            videoFollowUpList = list(map(list, zip(*Data)))
            sortLog.debug("List made from video follow up data")
            quota, inDB = pt.getQuotaUsed(projectID)
            sortLog.info(f"Used Quota obtained! So far incurred: {quota}")
            try:
                activeCredentials = pt.getCredentials(portNumber, clientSecretFile)
                sortLog.info("Credentials obtained!")
                youtube = build("youtube", "v3", credentials=activeCredentials)
                sortLog.info("Youtube object built!")
                youtubeWatchLater, requestOps = pt.getWatchLater(youtube, playlistID, True)
                sortLog.info(f"Youtube Watchlater Obtained! Quota incurred: {requestOps}")
                quota += requestOps
                sortLog.info(f"Youtube Watchlater Obtained! Quota incurred: {requestOps}, Total: {quota}")
                try: 
                    sortedWatchLater = pt.sortWatchLater(youtubeWatchLater, creatorDictionary, keywordDictionary, numberedSerializedKeywords, serializedKeywords, videoFollowUpList, sequentialCreators)
                    sortLog.info("Watchlater sorted!")
                    try:
                        videoOps, youtubeWatchLater = pt.updatePlaylist(youtubeWatchLater, sortedWatchLater, youtube, playlistID)
                        quota += videoOps*50
                        sortLog.info(f"Watchlater updated on youtube! Quota incurred: {videoOps*50}, Total: {quota}")
                        youtubeWatchLater = pt.renumberWatchLater(youtubeWatchLater)
                        sortLog.debug("Watchlater renumbered for DB storage!")
                        msg = "Sorted"
                    except Exception as e:
                        msg = "Error updating yt watch later"
                        sortLog.error(f"Error: {e}")
                except Exception as e:
                    msg = "Error sorting Watch Later"
                    sortLog.error(f"Error: {e}")
            except Exception as e:
                msg = "Error getting yt credentials or watchlater list"
                sortLog.error(f"Error: {e}")
        except Exception as e:
            msg =  "Error getting data from DB"
            sortLog.error(f"Error: {e}")
        try:
            pt.storeWatchLaterDB(youtubeWatchLater)
            sortLog.info("Watchlater stored in DB for stats!")
            pt.setQuotaUsed(inDB, quota, 1)
            sortLog.info(f"Used Quota set! Total accrude: {quota}")
            pt.CloseDBconnnection()
            sortLog.info(f"Database connection closed!")
            msg = "& Stored!"
        except Exception:
            if msg != "":
                msg += " & "
            msg += "Error setting data in DB"
        return msg
@app.route('/renew', methods=['GET'])  
def reNewToken():
    if request.method == 'GET':
        flow = pt.getFlowObject( clientSecretFile)
        flow.run_local_server()
        flow.authorized_session()
        credentials = flow.credentials
        pt.saveCredentails(credentials)
    return 'renewed!'

database, mariaPort, password, serverIp, user, projectID, portNumber, playlistID, clientSecretFile, hostIP, hostPort = pt.getProjectVariables("config.yaml")

numberedSerializedKeywords = ['series', 'part', 'finale', 'episode', 'ep', 'smarter every day']
serializedKeywords = ['finale']
sequentialCreators = ['Wintergatan', 'LegalEagle','penguinz0', 'AntsCanada', 'Brozime', 'FE-Engineer']

pt.getDataBaseConnection(user, password, serverIp, mariaPort, database)

app.run(host=hostIP, port=hostPort)