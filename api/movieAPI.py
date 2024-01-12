import uuid
import time
from flask import Blueprint, request, jsonify
from firebase_admin import firestore, storage
import requests
from io import BytesIO
from PIL import Image
import traceback
import sys
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

scope = [
  "https://spreadsheets.google.com/feeds",
  'https://www.googleapis.com/auth/spreadsheets',
  "https://www.googleapis.com/auth/drive.file",
  "https://www.googleapis.com/auth/drive"
]
credentials = ServiceAccountCredentials.from_json_keyfile_name(
  'api/credentials.json', scope)
db = firestore.client()
bucket = storage.bucket()

movie_ref = db.collection('movie_info')
recent_movie_ref = db.collection('recent_movies')

movieAPI = Blueprint('movieAPI', __name__)

imageLinks = {}

expiryTime = int(round(time.time() * 1000))

listDatas = ""


def getSheet():
  gc = gspread.authorize(credentials)
  wks = gc.open('Jav Movies').sheet1
  return wks


def getValueAsArray(obj):
  actName = obj['actName']
  downloadLink = obj['downloadLink']
  imageComLink = obj['imageComLink']
  imageLink = obj['imageLink']
  mvId = obj['mvId']
  name = obj['name']
  rating = obj['rating']
  releaseDate = obj['releaseDate']
  subLink = obj['subLink']
  return [
    mvId, name, actName, imageLink, imageComLink, downloadLink, subLink,
    rating, releaseDate
  ]


@movieAPI.route('/addMv', methods=['POST'])
def create():
  wks = getSheet()
  try:
    start_time = time.time()
    current_time_ms = int(round(time.time() * 1000))
    payload = request.json
    payload = {
      **payload, "mvId": current_time_ms,
      "imageComLink": getImgCompressedUrl(payload)
    }
    data = getValueAsArray(payload)
    wks.append_row(data)
    end_time = time.time()
    print("Time Taken for Add: ", end_time - start_time, " seconds")
    return jsonify({"mvDetails": [payload]}), 200
  except Exception as e:
    return f"An Error Occured : {e}"


@movieAPI.route('/deleteByRating', methods=['DELETE'])
def deleteByRating():
  wks = getSheet()
  datas = wks.get_all_values()
  isItSuccess = True
  try:
    for row in datas:
      mvId = row[0]
      name = row[1] or ""
      imageLink = row[3] or ""
      rating = int(row[7]) or 0
      if rating > 0 and rating < 4:
        print("Movie Ref for Delete:", name, rating)
        try:
          if imageLink.find('faleno') == -1:
            blob = bucket.blob(name + '.jpg')
            blob.delete()
          cell = wks.find(str(mvId))
          row_number = cell.row
          wks.delete_row(row_number)
        except Exception as e:
          isItSuccess = False
          print("Error Occurred During Image and Object Delete:", name)
          print("Error:", e)
    return jsonify({'success': isItSuccess}), 200 if isItSuccess else 400
  except Exception as e:
    return f"An Error Occured : {e}"


@movieAPI.route('/update', methods=['POST'])
def update():
  wks = getSheet()
  try:
    payload = request.json
    payload = {**payload, "imageComLink": getImgCompressedUrl(payload)}
    cell = wks.find(str(payload['mvId']))
    row_number = cell.row
    data = getValueAsArray(payload)
    start_time = time.time()
    wks.update(f"A{row_number}:I{row_number}", [data])
    end_time = time.time()
    print("Time Taken for Update: ", end_time - start_time, " seconds")
    return jsonify({'mvDetails': [payload]}), 200
  except Exception as e:
    return f"An Error Occured : {e}"


@movieAPI.route('/list')
def getList():
  wks = getSheet()
  start_time = time.time()
  datas = wks.get_all_values()
  end_time = time.time()
  print("Time Taken for Get: ", end_time - start_time, " seconds")
  response = []
  try:
    start_time = time.time()
    for row in datas:
      mvId = row[0]
      name = row[1] or ""
      actName = row[2] or ""
      imageLink = row[3] or ""
      imageComLink = row[4] or ""
      downloadLink = row[5] or ""
      subLink = row[6] or ""
      rating = int(row[7]) or 0
      releaseDate = row[8] or ""
      response.append({
        "mvId": int(mvId),
        "name": name,
        "actName": actName,
        "imageLink": imageLink,
        "imageComLink": imageComLink,
        "downloadLink": downloadLink,
        "subLink": subLink,
        "rating": rating,
        "releaseDate": releaseDate
      })
    print("Response Givent")
    end_time = time.time()
    print("Time Taken for Get Process: ", end_time - start_time, " seconds")
    return jsonify({"mvDetails": response}), 200
  except Exception as error:
    print("Error Occurred", error)
  return jsonify({"Status": "Internal Server Error"}), 500


def printProgressBar(i, max, fileName=''):
  n_bar = 10  #size of progress bar
  j = i / max
  sys.stdout.write('\r')
  sys.stdout.write(
    f"[{'=' * int(n_bar * j):{n_bar}s}] {int(100 * j)}% {fileName}")
  sys.stdout.flush()


def getImgCompressedUrl(payload):
  url = payload['imageLink']
  name = payload['name']
  imageComLink = payload.get('imageComLink', '')
  print('ImageComLink', imageComLink)
  if imageComLink != None and imageComLink.find(
      'firebasestorage.googleapis.com') != -1:
    print("ImageConmLink Present", name)
    return imageComLink
  try:
    print("ImageComLink Not Present", name)
    if url.find('faleno') != -1:
      return url + '?output-quality=15'
    else:
      response = requests.get(url)
      img = Image.open(BytesIO(response.content))
      img.save(name + '.jpg', format='JPEG', optimize=True, quality=50)
      blob = bucket.blob(name + '.jpg')
      blob.upload_from_filename(name + '.jpg')
      url = 'https://firebasestorage.googleapis.com/v0/b/ssproject-376507.appspot.com/o/' + name + '.jpg'
      response = requests.get(url)
      if os.path.exists(name + '.jpg'):
        os.remove(name + '.jpg')
      if response.ok:
        data = response.json()
        return url + '?alt=media&token=' + data['downloadTokens']
    return ''
  except:
    traceback.print_exc()


def updateListOfDatas(movies):
  counter = 0
  total_movies = len(movies)
  print("Total Movies:", total_movies)
  mvName = ''
  for movie in movies:
    # payload = {**movie, "imageComLink": getImgCompressedUrl(movie)}
    # mv_ref = movie_ref.where('mvId', '==', payload['mvId']).get()
    # movie_ref.document(mv_ref[0].id).update(payload)
    imageComLink = movie.get('imageComLink', '')
    if imageComLink == '':
      mvName += movie.get('name', '') + ','
    counter += 1
    printProgressBar(counter, total_movies, '')
  # print('mvNames:', mvName)


@movieAPI.route('/addRecent', methods=['POST'])
def createRecent():
  try:
    id = uuid.uuid4()
    payload = request.json
    docs = recent_movie_ref.stream()
    for doc in docs:
      doc.reference.delete()
    recent_movie_ref.document(id.hex).set(payload)
    return jsonify({"mvDetails": [payload]}), 200
  except Exception as e:
    return f"An Error Occured : {e}"


# @movieAPI.route('/addMv', methods=['POST'])
# def create():
#   global listDatas
#   try:
#     id = uuid.uuid4()
#     current_time_ms = int(round(time.time() * 1000))
#     payload = request.json
#     payload = {
#       **payload, "mvId": current_time_ms,
#       "imageComLink": getImgCompressedUrl(payload)
#     }
#     print(payload)
#     movie_ref.document(id.hex).set(payload)
#     if listDatas != "":
#       listDatas[payload['mvId']] = payload
#     return jsonify({"mvDetails": [payload]}), 200
#   except Exception as e:
#     return f"An Error Occured : {e}"


@movieAPI.route('/getRecentList')
def getRecentList():
  # global expiryTime
  # global imageLinks
  try:
    recent_movie_ref = db.collection('recent_movies')
    recent_movies = [doc.to_dict() for doc in recent_movie_ref.stream()]
    print("Recent Movies", recent_movies)

    # updateListOfDatas(all_movies)
    return jsonify(recent_movies), 200
  except Exception as e:
    return f"An Error Occured : {e}"


# @movieAPI.route('/list')
# def read():
#   # global expiryTime
#   # global imageLinks
#   global listDatas
#   try:
#     field = request.args.get("field")
#     type = request.args.get("type")
#     movieList_ref = movie_ref
#     directory = "/home/runner/javmov/"
#     for filename in os.listdir(directory):  # Delete jpg Files
#       print('FileName :', filename)
#       if filename.endswith(".jpg"):
#         os.remove(os.path.join(directory, filename))
#     # current_time = int(round(time.time() * 1000))
#     # print("Expiry Time:", expiryTime, ",Current Time:", current_time)
#     # files = bucket.list_blobs()
#     # cExpiryTime = current_time + (7 * 24 * 60 * 60 * 1000)
#     # print("ExpiryTime :", expiryTime, ",cExpiryTime:", cExpiryTime)
#     # for file in files:
#     #   if current_time < expiryTime and file.name in imageLinks:
#     #     url = imageLinks[file.name]
#     #     print("If Coming", file.name)
#     #   else:
#     #     url = file.generate_signed_url(expiration=cExpiryTime)
#     #     imageLinks[file.name] = url
#     #     print("else Coming", url)
#     # expiryTime = cExpiryTime
#     print(listDatas == "", "List Datas Updated")
#     if listDatas == "":
#       listDatas = {}
#       if type != None:
#         type = 'ASCENDING' if type == 'asc' else 'DESCENDING'
#         movieList_ref = db.collection('movie_info').order_by(field,
#                                                              direction=type)
#       all_movies = [doc.to_dict() for doc in movieList_ref.stream()]
#       print("List Datas Updated")
#       for movie in all_movies:
#         listDatas[movie['mvId']] = movie
#     # updateListOfDatas(all_movies)
#     return jsonify({"mvDetails": list(listDatas.values())}), 200
#   except Exception as e:
#     return f"An Error Occured : {e}"


@movieAPI.route('/getByRating')
def getByRating():
  # global expiryTime
  # global imageLinks
  global listDatas
  try:
    rating = request.args.get("rating")
    movieList_ref = movie_ref
    directory = "/home/runner/javmov/"
    for filename in os.listdir(directory):  # Delete jpg Files
      print('FileName :', filename)
      if filename.endswith(".jpg"):
        os.remove(os.path.join(directory, filename))
    # current_time = int(round(time.time() * 1000))
    # print("Expiry Time:", expiryTime, ",Current Time:", current_time)
    # files = bucket.list_blobs()
    # cExpiryTime = current_time + (7 * 24 * 60 * 60 * 1000)
    # print("ExpiryTime :", expiryTime, ",cExpiryTime:", cExpiryTime)
    # for file in files:
    #   if current_time < expiryTime and file.name in imageLinks:
    #     url = imageLinks[file.name]
    #     print("If Coming", file.name)
    #   else:
    #     url = file.generate_signed_url(expiration=cExpiryTime)
    #     imageLinks[file.name] = url
    #     print("else Coming", url)
    # expiryTime = cExpiryTime
      ratedDatas = {}
      if type != None:
        movieList_ref = db.collection("movie_info").where(
          "rating", ">", rating).order_by("rating", "DESCENDING")
      all_movies = [doc.to_dict() for doc in movieList_ref.stream()]
      print("List Datas Updated")
      for movie in all_movies:
        ratedDatas[movie['mvId']] = movie
    # updateListOfDatas(all_movies)
    return jsonify({"mvDetails": list(ratedDatas.values())}), 200
  except Exception as e:
    return f"An Error Occured : {e}"


# @movieAPI.route('/update', methods=['POST'])
# def update():
#   global listDatas
#   try:
#     payload = request.json
#     payload = {**payload, "imageComLink": getImgCompressedUrl(payload)}
#     mv_ref = movie_ref.where('mvId', '==', payload['mvId']).get()
#     movie_ref.document(mv_ref[0].id).update(payload)
#     print("Payload", payload)
#     if listDatas != "":
#       print("Payload", payload)
#       listDatas[payload['mvId']] = payload
#     return jsonify({'mvDetails': [payload]}), 200
#   except Exception as e:
#     return f"An Error Occured : {e}"

# @movieAPI.route('/deleteByRating', methods=['DELETE'])
# def deleteByRating():
#   global listDatas
#   isItSuccess = True
#   items_to_delete = []
#   try:
#     for movie in list(listDatas.values()):
#       name = movie['name']
#       mvId = movie['mvId']
#       imageLink = movie['imageLink']
#       rating = int(movie['rating'])
#       if rating > 0 and rating < 4:
#         print("Movie Ref for Delete:", name, rating)
#         mv_ref = movie_ref.where('mvId', '==', mvId).get()
#         try:
#           if imageLink.find('faleno') == -1:
#             blob = bucket.blob(name + '.jpg')
#             blob.delete()
#           movie_ref.document(mv_ref[0].id).delete()
#           items_to_delete.append(mvId)
#         except Exception as e:
#           isItSuccess = False
#           print("Error Occurred During Image and Object Delete:", name)
#           print("Error:", e)
#     for item in items_to_delete:
#       del listDatas[item]
#     return jsonify({'success': isItSuccess}), 200 if isItSuccess else 400
#   except Exception as e:
#     return f"An Error Occured : {e}"


@movieAPI.route('/delete/<mvId>', methods=['DELETE'])
def delete(mvId):
  global listDatas
  try:
    mvId = int(mvId) if int(mvId) > 55 else mvId
    mv_ref = movie_ref.where('mvId', '==', mvId).get()
    name = movie_ref.document(mv_ref[0].id).get().to_dict()["name"]
    print("Movie Ref for Delete: ", name)
    try:
      blob = bucket.blob(name + '.jpg')
      blob.delete()
    except:
      print("Error Occured During Image Delete.", blob)
    movie_ref.document(mv_ref[0].id).delete()
    del listDatas[mvId]
    return jsonify({'success': True}), 200
  except Exception as e:
    return f"An Error Occured : {e}"


@movieAPI.route('/search')
def searchMvDetails():
  try:
    query = request.args.get("searchStr")
    print("Search Query:", query)
    searchResults = movie_ref.where('name', 'array_contains_any',
                                    [query.lower()]).get()
    print("searchResults:", searchResults)
    all_movies = [doc.to_dict() for doc in searchResults.stream()]
    return jsonify({"mvDetails": all_movies}), 200
  except Exception as e:
    return f"An Error Occured : {e}"


@movieAPI.route('/importMv', methods=['POST'])
def importMv():

  # batch = db.batch()
  # for existVal in existArr:
  #   movieDetail = existVal.split(",")
  #   doc_ref = movie_ref.document()
  #   batch.set(
  #     doc_ref, {
  #       "mvId": movieDetail[0],
  #       "name": movieDetail[1],
  #       "actName": movieDetail[2],
  #       "imageLink": movieDetail[3],
  #       "downloadLink": movieDetail[4],
  #       "subLink": movieDetail[5],
  #       "rating": movieDetail[6],
  #     })
  #   batch.commit()
  print("There is no data available for import")
  return {'success': True}, 200
