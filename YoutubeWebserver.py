from flask import Flask, request, abort, render_template, flash
import xmltodict as x2d
import pywertube as pt
import uuid
import mariadb
import os
from icecream import ic
from datetime import datetime as dt, timedelta
from googleapiclient.discovery import build
import json

#Set Logger
if 'TERM_PROGRAM' in os.environ.keys() and os.environ['TERM_PROGRAM'] == 'vscode':
    logger = pt.initLogger(__file__, debug=True, verbose=False)
elif SECRET_KEY:
    logger = pt.initLogger(__file__, debug=os.environ.get('DEBUG_MODE',False), verbose=os.environ.get('VERBOSE_DEBUG', False))
else:
    logger = pt.initLogger(__file__, debug=False, verbose=False)
storedCreators = None

#Initalize Base Variables
database, mariaPort, password, serverIp, user, host_ip, host_port, projectID, portNumber, playlistID = pt.getProjectVariablesENV()

client_secret_dict = {'web':{
                            'client_id':os.environ.get('CLIENT_ID'),
                            'project_id':os.environ.get('PROJECT_ID'),
                            'auth_uri':os.environ.get('AUTH_URI'),
                            'token_uri':os.environ.get('TOKEN_URI'),
                            'auth_provider_x509_cert_url':os.environ.get('AUTH_PROVIDER'),
                            'client_secret':os.environ.get('CLIENT_SECRET'),
                            'redirect_uris':os.environ.get('REDIRECT_URIS').split(',')}}

pt.createJsonFile('youtube_user_client_secret.json', client_secret_dict)

numberedSerializedKeywords = ['series', 'part', 'finale', 'episode', 'ep', '#', 'chapter']
serializedKeywords = []

app = Flask(__name__)
app.secret_key = b'_5#y2L"F4Q8z\n\xec]/'
pt.getDataBaseConnection(user, password, serverIp, mariaPort, database)

sequentialCreators_tuple_list = pt.getDataDB('SequentialCreators',['Creators'], 'left  join Creators on SequentialCreators.creatorId = Creators.id')
sequentialCreators = [''.join(i) for i in sequentialCreators_tuple_list]

@app.route('/', methods=('GET','POST'))
def index():
    if request.method == 'POST':
        sort()
    return render_template('index.html')

@app.route('/about')
def about():
    return render_template('about.html')

@app.route('/sorting_editor')
def sortEditor():
    return render_template('SortEditor.html')

@app.route('/data_visualization')
def dataVis():
    return render_template('dataVis.html')

@app.route('/webhook', methods=['POST', 'GET'])
def webhook():
    webhookLog = logger.bind()
    pt.setLogger(webhookLog)
    webhookLog.debug("webhooklog set as logger!")
    if request.method == 'POST':
        pt.pickleSomething(request.data, "request_"+dt.now().strftime("%Y%m%d%H%M%S"))
        # if not pt.checkTypeReturn(request.data, dict):
        #     data = json.loads(request.data)
        # else:
        #     data = x2d.parse(request.data)
        # entry = data["feed"]["entry"]
        # if pt.checkTypeReturn(entry,dict):
        #     if dt.today().strftime("%Y-%m-%d") == entry["published"].split("T")[0]:
        #         try:
        #             creatorDictionary, keywordDictionary, videoFollowUpList, quota, inDB = initWatchLater(webhookLog)
        #             DBwatchlater = pt.getDataDB('WatchLaterList',['*'])
        #             try:
        #                 uncondictionalBool = dict(pt.getDataDB('Creators', ['creators','unconditional']))
        #                 if bool(int.from_bytes(uncondictionalBool["PewDiePie"],"big")):
        #                     try:
        #                         youtube = getYoutubeObj(logger)
        #                         try:
        #                             videoDict = pt.getVideoYT(youtube, entry["yt:videoId"])
        #                             quota += 1
        #                             try:
        #                                 videoTuple = (len(DBwatchlater), '', entry["yt:videoId"], videoDict["duration"], videoDict["creator"], videoDict["published"], videoDict["title"])
        #                                 DBwatchlater.append(videoTuple)
        #                                 sortedWatchLater = pt.sortWatchLater(DBwatchlater, creatorDictionary, keywordDictionary, numberedSerializedKeywords, serializedKeywords, videoFollowUpList, sequentialCreators)
        #                                 webhookLog.info("Watchlater sorted!")
        #                                 try:    
        #                                     postion = sortedWatchLater.index(videoTuple)
        #                                     try:
        #                                         pt.insertVideoYT(youtube, playlistID, entry["yt:videoId"], postion)
        #                                         quota += 50
        #                                         webhookLog.info("YT video inserted!")
        #                                         try:
        #                                             pt.storeWatchLaterDB(sortedWatchLater)
        #                                             webhookLog.info("Watchlater stored in DB for stats!")
        #                                             datetime = dt.now().strftime('%Y-%m-%d %H:%M:%S')
        #                                             pt.WatchLaterStats(sortedWatchLater, datetime)
        #                                             quota += pt.WatchLaterCreatorStats(sortedWatchLater, datetime, youtube)
        #                                         except Exception as e:
        #                                             webhookLog.error(f"Error getting index of stored video: {e}")
        #                                     except Exception as e:
        #                                         webhookLog.error(f"Error inserting Video into YT playylist: {e}")
        #                                 except Exception as e:
        #                                     webhookLog.error(f"Error getting index of stored video: {e}")
        #                             except Exception as e:
        #                                 webhookLog.error(f"Error sorting watch later: {e}")
        #                         except Exception as e:
        #                             webhookLog.error(f"Error getting video info from YT: {e}")
        #                     except Exception as e:
        #                         webhookLog.error(f"Error getting youtube obj: {e}")
        #             except Exception as e:
        #                 webhookLog.error(f"Error getting uncondictional boolean: {e}")
        #         except Exception as e:
        #             webhookLog.error(f"Error getting inital data to insert: {e}")                    
                
        #         try:
        #             pt.setQuotaUsed(inDB, quota, 1)
        #             webhookLog.info(f"Used Quota set! Total accrude: {quota}")
        #             webhookLog.info("Watch later stats stored")
        #             pt.CloseDBconnnection()
        #             webhookLog.info(f"Database connection closed!")
        #         except Exception as e:
        #             webhookLog.error(f"Error setting data in DB: {e}")
                

        #         ic(f"youtube video '{entry['title']}' released by {entry['author']['name']}!")
        #         ic(data)
        #     #    if entry["author"]["name"] not in storedCreators:
        #     #        pt.insertCreator(entry["author"]["name"], entry["yt:channelId"])
        #     #        storedCreators.append(entry["author"]["name"])
        #     #    pt.insertTime(storedCreators.index(entry["author"]["name"])+1, entry["published"], entry["yt:videoId"])
        #     else:
        #         ic(f"{entry['author']['name']} has updated video '{entry['title']}'!")
        return {"message": "Accepted"}, 202
        # else:
        #     return {"message": "Not Accepted"}, 406
    else:
        return request.args.get('hub.challenge')

@app.route('/subs', methods=['GET'])
def subscribe():
    subLog = logger.bind()
    pt.setLogger(subLog)
    subLog.info("subLogger set as logger!")
    activeCredentials = pt.getCredentials(portNumber, 'youtube_user_client_secret.json')
    subLog.info("Credentials obtained!")
    youtube = build("youtube", "v3", credentials=activeCredentials)
    subLog.info("Youtube object built!")
    subs = pt.getSubscriptions(youtube, mine=True)
    pt.storeSubscripton(subs, youtube)
    pt.subscribeCreators()
    return "subscribers updated"

#@app.route('/sort', methods=['POST'])
def sort():
    #pt.structlog.threadlocal.clear_threadlocal()
    #pt.structlog.threadlocal.bind_threadlocal(
    #    view=request.path,
    #    request_id=str(uuid.uuid4()),
    #    peer=request.access_route[0],data
    #)
    msg = ""
    sortLog = logger.bind()
    pt.setLogger(sortLog)
    sortLog.info("sortLogger set as logger!")
    if request.method == 'POST':
        sortLog.info("Entering POST Request")
        try:
            creatorDictionary, keywordDictionary, videoFollowUpList, quota, inDB = initWatchLater(sortLog)
            try:
                youtube = getYoutubeObj(logger)
                youtubeWatchLater, requestOps = pt.getWatchLater(youtube, playlistID, True)
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
                        msg = "Sorted! \n"
                        try:
                            pt.storeWatchLaterDB(youtubeWatchLater)
                            sortLog.info("Watchlater stored in DB for stats!")
                            datetime = dt.now().strftime('%Y-%m-%d %H:%M:%S')
                            pt.WatchLaterStats(youtubeWatchLater, datetime)
                            quota += pt.WatchLaterCreatorStats(youtubeWatchLater, datetime, youtube)
                            msg += "Stored!\n"
                        except Exception as e:
                            msg += "Error storing stats!\n"
                            sortLog.error(f"Error: {e}")
                    except Exception as e:
                        msg += "Error updating yt watch later!\n"
                        sortLog.error(f"Error: {e}")
                except Exception as e:
                    msg += "Error sorting Watch Later!\n"
                    sortLog.error(f"Error: {e}")
            except Exception as e:
                msg += "Error getting yt credentials or watchlater list!\n"
                sortLog.error(f"Error: {e}")
        except Exception as e:
            msg +=  "Error getting data from DB!\n"
            sortLog.error(f"Error: {e}")
        try:
            pt.setQuotaUsed(inDB, quota, 1)
            sortLog.info(f"Used Quota set! Total accrude: {quota}")
            sortLog.info("Watch later stats stored")
            msg += f"Quota Saved! Accruded: {quota}\n"
            pt.CloseDBconnnection()
            sortLog.info(f"Database connection closed!")
        except Exception:
            msg += "Error setting data in DB \n"
        flash(msg)
@app.route('/renew', methods=['GET'])  
def reNewToken():
    if request.method == 'GET':
        flow = pt.getFlowObject('youtube_user_client_secret.json')
        flow.run_local_server()
        flow.authorized_session()
        credentials = flow.credentials
        pt.saveCredentails(credentials)
    return 'renewed!'

def initWatchLater(logger):
    pt.getDataBaseConnection(user, password, serverIp, mariaPort, database)
    logger.info("DataBase Connection made!")
    Data = pt.getDataDB("Creators", ["creators", "priorityScore"])
    logger.info("Creators and their Priority Score obtained!")
    creatorDictionary = dict(Data)
    logger.debug("Dictionary made from creator and priority score")
    Data = pt.getDataDB("Keyphrases", ["phrase", "score"])
    logger.info("Keyphrase and their Priority Score obtained!")
    keywordDictionary = dict(Data)
    logger.debug("Dictionary made from keyphrase and priority score")
    Data = pt.getDataDB("OrderVideos", ["id", "videoID", "predecentVideoID"])
    logger.info("Follow up video data obtained!")
    videoFollowUpList = list(map(list, zip(*Data)))
    logger.debug("List made from video follow up data")
    quota, inDB = pt.getQuotaUsed(projectID)
    logger.info(f"Used Quota obtained! So far incurred: {quota}")
    return creatorDictionary, keywordDictionary, videoFollowUpList, quota, inDB

def getYoutubeObj(logger):
    activeCredentials = pt.getCredentials(portNumber, 'youtube_user_client_secret.json')
    logger.info("Credentials obtained!")
    youtube = build("youtube", "v3", credentials=activeCredentials)
    logger.info("Youtube object built!")
    return youtube

if __name__ == "__main__":
    app.run(host=host_ip, port=host_port)
    