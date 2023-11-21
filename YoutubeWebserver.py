from flask import Flask, request, abort
import xmltodict as x2d
import pywertube as pt
from datetime import datetime as dt, timedelta
from googleapiclient.discovery import build

app = Flask(__name__)
dbConnection = None
storedCreators = None

@app.route('/webhook', methods=['POST', 'GET'])
def webhook():
    if request.method == 'POST':
        data = x2d.parse(request.data)
        entry = data["feed"]["entry"]

        if dt.today().strftime("%Y-%m-%d") == entry["published"].split("T")[0]:
            if entry["author"]["name"] not in storedCreators:
                pt.insertCreator(dbConnection, entry["author"]["name"], entry["yt:channelId"])
                storedCreators.append(entry["author"]["name"])
            pt.insertTime(dbConnection, storedCreators.index(entry["author"]["name"])+1, entry["published"], entry["yt:videoId"])
        else:
            print(entry["author"]["name"] + " has updated a video!")
        return request.data
    else:
        return request.args.get('hub.challenge')

@app.route('/sort', methods=['GET'])
def sort():
    if request.method == 'GET':
        dbConnection = pt.getDataBaseConnection(user, password, serverIp, mariaPort, database)
        Data = pt.getDataDB(dbConnection, "Creators", "creators", "priorityScore")
        creatorDictionary = dict(Data)
        Data = pt.getDataDB(dbConnection, "Keyphrases", "phrase", "score")
        keywordDictionary = dict(Data)
        Data = pt.getDataDB(dbConnection, "OrderVideos", "id", "videoID", "predecentVideoID")
        videoFollowUpList = list(map(list, zip(*Data)))
        quota, dbDate = pt.getQuotaAmount(dbConnection,projectID)

        activeCredentials = pt.getCredentials(portNumber)
        youtube = build("youtube", "v3", credentials=activeCredentials)
        youtubeWatchLater, requestOps = pt.getWatchLater(youtube, playlistID, True)
        quota += requestOps
        sortedWatchLater = pt.sortWatchLater(youtubeWatchLater, creatorDictionary, keywordDictionary, numberedSerializedKeywords, serializedKeywords, videoFollowUpList, sequentialCreators)
        videoOps, youtubeWatchLater = pt.updatePlaylist(youtubeWatchLater, sortedWatchLater, youtube, playlistID)
        youtubeWatchLater = pt.renumberWatchLater(youtubeWatchLater)
        quota += videoOps*50

        pt.storeWatchLater(dbConnection, sortedWatchLater)

        print("Quota cost incurred: " + str(quota))
        pt.saveQuota(dbConnection, dbDate, quota, 1)
        return 'Sorted!'

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
sequentialCreators = ['Wintergatan']

dbConnection = pt.getDataBaseConnection(user, password, serverIp, mariaPort, database)

app.run(host=hostIP, port=hostPort)