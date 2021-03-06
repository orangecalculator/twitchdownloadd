#!/usr/bin/env python3


# MIT License
# 
# Copyright (c) 2021 orangecalculator
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import requests
import json
import time
from datetime import datetime
import dateutil.parser
import re
import os
import concurrent.futures

v5headers = {
    "Client-ID": "37v97169hnj8kaoq8fs3hzz8v6jezdj",
    "Accept": "application/vnd.twitchtv.v5+json",
}

gqlurl = "https://gql.twitch.tv/gql"
gqlheaders = {
    "Client-ID": "kimne78kx3ncx6brgo4mv6wki5h1ko",
}

header = {
    "v5": v5headers,
    "gql": gqlheaders,
}

def query_playbackaccesstoken(videoid):
    ##{
    ##    "operationName": "PlaybackAccessToken",
    ##    "variables": {
    ##        "isLive": false,
    ##        "login": "",
    ##        "isVod": true,
    ##        "vodID": "SAMPLEID",
    ##        "playerType": "channel_home_live"
    ##    },
    ##    "extensions": {
    ##        "persistedQuery": {
    ##            "version": 1,
    ##            "sha256Hash": "0828119ded1c13477966434e15800ff57ddacf13ba1911c129dc2200705b0712"
    ##        }
    ##    }
    ##}
    payload = '{"operationName": "PlaybackAccessToken", "variables": {"isLive": false, "login": "", "isVod": true, "vodID": "%s", "playerType": "channel_home_live"}, "extensions": {"persistedQuery": {"version": 1, "sha256Hash": "0828119ded1c13477966434e15800ff57ddacf13ba1911c129dc2200705b0712"}}}' % videoid

    res = requests.post(gqlurl, headers=gqlheaders, data=payload)
    res.raise_for_status()
    
    res_data = res.json()
    res_data_token = res_data["data"]["videoPlaybackAccessToken"]
    
    tokenstr = res_data_token["value"]
    signature = res_data_token["signature"]

    return tokenstr, signature

def query_master_m3u8(videoid, tokenstr, signature):
    URL = f"https://usher.ttvnw.net/vod/{videoid}.m3u8"

    params = {
        "nauth": tokenstr, 
        "nauthsig": signature, 
        "allow_source": "true",
        "player": "twitchweb", 
        "allow_spectre": "true",
        "allow_audio_only": "true", 
    }

    res = requests.get(URL, params=params)
    res.raise_for_status()
    
    return res.text

def get_master_m3u8(videoid):
    tokenstr, signature = query_playbackaccesstoken(videoid)
    master_m3u8 = query_master_m3u8(videoid, tokenstr, signature)
    
    return master_m3u8
    
re_key_value = re.compile(r"""((?P<key>[\-A-Z]+)=(?P<value>("([^"]|\\")*")|([^",]*)))""")
def parse_master_m3u8(master_m3u8):
    master_m3u8 = master_m3u8.split('\n')
    
    videos = dict()
    
    k = 1
    while k < len(master_m3u8):
        line = master_m3u8[k]
        if line.startswith("#EXT-X-STREAM-INF:"):
            videoentry = dict()
            for pair in re_key_value.split(line.removeprefix("#EXT-X-STREAM-INF:")):
                if pair is None:
                    continue
                match = re_key_value.fullmatch(pair)
                if match is not None:
                    key = match.group("key")
                    value = match.group("value").removeprefix('"').removesuffix('"')
                    videoentry[key] = value
            k += 1
            url = master_m3u8[k]
            videoentry["url"] = url

            videos[videoentry["VIDEO"]] = videoentry
        
        k += 1
    
    return videos

def get_best_quality(videos):
    maxbandwidth = -1
    maxquality = None
    for quality, videoentry in videos.items():
        bandwidth = int(videoentry["BANDWIDTH"])
        if maxbandwidth < bandwidth:
            maxbandwidth = bandwidth
            maxquality = quality
    return maxquality

def parse_playlist_m3u8(playlist_m3u8):
    playlist_m3u8 = playlist_m3u8.split('\n')

    files = []

    k = 1
    while k < len(playlist_m3u8):
        line = playlist_m3u8[k]
        if line.startswith("#EXTINF:"):
            k += 1
            filename = playlist_m3u8[k]
            files.append(filename)
        k += 1
    
    return files

def join_video(playlist_path, outputfilename):
    import subprocess
    import posixpath

    # ffmpeg only recognize posix path
    playlist_path = posixpath.normpath(playlist_path.replace('\\', '/'))

    command = [
        "ffmpeg",
        "-i", playlist_path,
        "-c", "copy",
        outputfilename,
        "-stats",
        "-loglevel", "warning",
    ]
    
    result = subprocess.run(command)

re_urlbase = re.compile("/[^/]*$")
def download_video_from_playlist_url(url_playlist, tmpdir, download_filename, max_workers=8, max_retries=5):
    query_playlist = requests.get(url_playlist)
    query_playlist.raise_for_status()

    playlist_m3u8 = query_playlist.text
    playlist_m3u8_filename = os.path.join(tmpdir, "playlist.m3u8")
    with open(playlist_m3u8_filename, "w") as f:
        f.write(playlist_m3u8)

    videofilelist = parse_playlist_m3u8(playlist_m3u8)
    url_base = re_urlbase.sub('/', url_playlist)

    def _download_video_part(url, filename):
        res = requests.get(url, stream=True)

        with open(filename, "wb") as f:
            for chunk in res.iter_content(chunk_size=None):
                f.write(chunk)
    
    def download_video_part(url, filename):
        if os.path.isfile(filename):
            return
        
        for _ in range(max_retries):
            try:
                _download_video_part(url, filename)
                return
            except requests.exceptions.RequestException as e:
                pass
        
        raise e
    
#    for filename in videofilelist:
#        download_video_part(os.path.join(url_base, filename), os.path.join(tmpdir, filename))
    futures_download = [(download_video_part, os.path.join(url_base, filename), os.path.join(tmpdir, filename)) for filename in videofilelist]
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures_download = [ex.submit(*args) for args in futures_download]
    
        print()
        complete = 0
        for task_complete in concurrent.futures.as_completed(futures_download):
            complete += 1
            print(f"\rDownload Status: {complete}/{len(futures_download)} with {max_workers} workers", end='')
        print()

    join_video(playlist_m3u8_filename, download_filename)

def get_users(username):
    URL = "https://api.twitch.tv/kraken/users"

    res = requests.get(URL, headers=v5headers, params={
        "login": username
    })
    res.raise_for_status()

    return res

def get_videos_channel(channelid):
    URL = f"https://api.twitch.tv/kraken/channels/{channelid}/videos"

    res = requests.get(URL, headers=v5headers)
    res.raise_for_status()

    return res

def get_stream(channelid):
    URL = f"https://api.twitch.tv/kraken/streams/{channelid}"

    res = requests.get(URL, headers=v5headers)
    res.raise_for_status()

    return res

def get_channelid(channelname):
    for channeldata in get_users(channelname).json()["users"]:
        if channeldata["name"] == channelname:
            return channeldata["_id"]

re_videofile = re.compile(r"(?P<date>\d*)_(?P<id>\d*)_(?P<streamer>\w*?)(_(?P<videoname>\w*))?\.(?P<videoext>\w*)")
allowed_ext = ["mkv", "mp4"]
def find_downloaded_videos():
    downloaded_videos = set()
    for filename in os.listdir():
        match_videofile = re_videofile.fullmatch(filename)
        if match_videofile and match_videofile.group("videoext") in allowed_ext:
            downloaded_videos.add(match_videofile.group("id"))
    return downloaded_videos

re_cached_video = re.compile(r"tmp_(?P<videoid>\d*)")
# defined for function download_video_by_master_m3u8
# re_master_m3u8_filename = re.compile(r"index_(?P<publishdate>\d*)_(?P<videoid>\d*)_(?P<channelname>.*).m3u8")
def find_cached_videos():
    cached_videos = dict()
    for filename in os.listdir():
        match_cached_video = re_cached_video.fullmatch(filename)
        if match_cached_video is None:
            continue

        cached_master_m3u8_filename = None
        for subfilename in os.listdir(filename):
            if re_master_m3u8_filename.fullmatch(subfilename) is not None:
                cached_master_m3u8_filename = subfilename
                break
        if cached_master_m3u8_filename is not None:
            videoid = match_cached_video.group("videoid")
            cached_videos[videoid] = os.path.join(filename, cached_master_m3u8_filename)
    
    return cached_videos


def tmpdir_video(video_id):
    tmpdir = f"tmp_{video_id}/"

    if not os.path.exists(tmpdir):
        os.mkdir(tmpdir)
    return tmpdir

re_master_m3u8_filename = re.compile(r"index_(?P<publishdate>\d*)_(?P<videoid>\d*)_(?P<channelname>.*).m3u8")
def download_video_by_master_m3u8(master_m3u8_filename):
    tmpdir = os.path.dirname(master_m3u8_filename)
    match = re_master_m3u8_filename.fullmatch(os.path.basename(master_m3u8_filename))

    publishdate = match.group("publishdate")
    videoid = match.group("videoid")
    channelname = match.group("channelname")

    video_filename = f"{publishdate}_{videoid}_{channelname}.mp4"

    if os.path.isfile(video_filename):
        return

    print(f"downloading video {videoid} to {video_filename}")

    with open(master_m3u8_filename, "r") as f:
        master_m3u8 = f.read()
    
    try:
        videolist = parse_master_m3u8(master_m3u8)
        quality = get_best_quality(videolist)
        download_video_from_playlist_url(
            videolist[quality]["url"],
            tmpdir,
            video_filename
        )
    except requests.exceptions.RequestException as e:
        print("exception occurred while downloading video by master_m3u8")
        print(e)
        return e

def download_videos(channelname, db, cache_only=False, **kwargs):
    channelid = db.get_channelid(channelname)
    
    try:
        stream_response = get_stream(channelid)
        stream_response = stream_response.json()

        is_live = (stream_response["stream"] is not None)
    except requests.exceptions.RequestException as e:
        print("exception occurred while fetching stream information")
        print(e)
        return

    try:
        videolist_response = get_videos_channel(channelid)
        videolist_response = videolist_response.json()
    except requests.exceptions.RequestException as e:
        print("exception occurred while fetching video list")
        print(e)
        return
    
    for video in videolist_response["videos"]:
        videoid = video["_id"].removeprefix('v')
        record = db.get_download_record(channelname, videoid)
        if record is None:
            publishdate = dateutil.parser.isoparse(video["published_at"]).astimezone().strftime("%Y%m%d")

            video_filename = f"""{publishdate}_{videoid}_{channelname}.mp4"""
            master_m3u8_filename = f"""index_{publishdate}_{videoid}_{channelname}.m3u8"""

            try:
                master_m3u8 = get_master_m3u8(videoid)
                tmpdir = tmpdir_video(videoid)
                master_m3u8_filename = os.path.join(tmpdir, master_m3u8_filename)
                with open(master_m3u8_filename, 'w') as f:
                    f.write(master_m3u8)
            except requests.exceptions.RequestException as e:
                print("exception occurred while fetching master playlist")
                print(e)
                continue

            print(f"video {videoid} will be downloaded to {video_filename}")

            record = {
                "videoid": videoid,
                "publishdate": publishdate,
                "status": "pending",
                "master_m3u8": master_m3u8_filename
            }
            db.set_download_record(channelname, videoid, record)
    
    if not is_live and not cache_only:
        for videoid in db.get_download_record_list(channelname):
            record = db.get_download_record(channelname, videoid)
            if record["status"] == "pending":
                try:
                    ret = download_video_by_master_m3u8(record["master_m3u8"])
                    if ret is not None:
                        break
                    record["status"] = "complete"
                    db.set_download_record(channelname, videoid, record)
                except requests.exceptions.RequestException as e:
                    print("exception occurred while downloading video")
                    print(e)

class channeldb:
    def __init__(self, dbfilename):
        self.dbfilename = dbfilename
        
        if os.path.isfile(self.dbfilename):
            with open(dbfilename, "r") as f:
                self.db = json.load(f)
        else:
            self.db = dict()
    
    def _sync_db(self):
        with open(self.dbfilename, "w") as f:
            json.dump(self.db, f, indent=4)
    
    def _fetch_channelinfo(self, channelname):
        if channelname not in self.db:
            channelid = get_channelid(channelname)

            self.db[channelname] = {
                "id": channelid,
                "downloaded": dict(),
            }
    
    def get_channelid(self, channelname):
        if channelname not in self.db:
            try:
                self._fetch_channelinfo(channelname)

                self._sync_db()
            except requests.exceptions.RequestException as e:
                print(f"fetching channel {channelname} info failed, ignoring")
                print(e)
                return

        return self.db[channelname]["id"]
    
    def set_streaming_status(self, channelname, status):
        self.db[channelname]["streaming"] = status
    
    def is_streaming(self, channelname):
        return self.db[channelname]["streaming"]
    
    def get_download_record_list(self, channelname):
        try:
            return list(self.db[channelname]["downloaded"].keys())
        except KeyError:
            return list()

    def get_download_record(self, channelname, videoid):
        try:
            return self.db[channelname]["downloaded"][videoid]
        except KeyError:
            return None
    
    def set_download_record(self, channelname, videoid, videoinfo=None):
        if channelname not in self.db:
            self._fetch_channelinfo(channelname)
        
        self.db[channelname]["downloaded"][videoid] = videoinfo
        with open(self.dbfilename, "w") as f:
            json.dump(self.db, f, indent=4)
    
    # Maybe clear entries in the future (i.e. exclude entries other than parameter channel_videos)

def do_parse_args(argv):
    import argparse
    parser = argparse.ArgumentParser()

    parser.add_argument("channelnames", nargs="+", help="channel names to download videos")

#    parser.add_argument("--id", help="channel id to use for download", action="store")
    parser.add_argument("--dbfile", "--db", help="db filename to use", action="store", type=str, default=".twitchdownloadd.db")
    parser.add_argument("--extra-headers", help="file containing extra headers", action="store", type=str)
    parser.add_argument("--cache-only", help="only cache master m3u8 list", action="store_true")
    parser.add_argument("--max-retry", help="number of retries when query fails", action="store", type=int, default=5)
    parser.add_argument("--max-workers", help="number of workers to use when downloading video", action="store", type=int, default=8)
    parser.add_argument("--polling-time", help="time period for polling", action="store", type=float, default=60)

    args = parser.parse_args()
    return args

#    if args.id is not None:
#        channelid = args.id
#    else:
#        channelid = get_channelid(args.channelname)
#    channelid = get_channelid(args.channelname)
#
#    config = {
#        "cache_only": args.cache_only,
#        "max_retry": args.max_retry,
#        "max_workers": args.max_workers,
#        "polling_time": args.polling_time,
#        "channelid": channelid,
#        "channelname": args.channelnames,
#    }
#
#    return config

def do_init_headers(headerfilename):
    with open(headerfilename, 'r') as f:
        extra_header = json.load(f)
    
    if not any(map(lambda headername: headername in extra_header.keys(), header.keys())):
        raise KeyError(f"valid extra headers include {', '.join(map(repr, header.keys()))}")

    for key, added_header_values in extra_header.items():
        header[key] |= added_header_values

def parse_args():
    import sys
    return do_parse_args(sys.argv)

def main():
    config = parse_args()
    db = channeldb(config.dbfile)

    if config.extra_headers is not None:
        do_init_headers(config.extra_headers)
    
    while True:
        for channelname in config.channelnames:
            download_videos(channelname, db, cache_only=config.cache_only, max_retry=config.max_retry, max_workers=config.max_workers)
        
        print(f"Sleeping at {datetime.now()}\r", end='')
        time.sleep(config.polling_time)

if __name__ == "__main__":
    main()
