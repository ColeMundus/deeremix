from pydeezer import Deezer, Downloader
from pydeezer.ProgressHandler import BaseProgressHandler
from pydeezer.constants import track_formats
from dataclasses import dataclass, field, asdict
import configparser
from tqdm import tqdm
from pprint import pprint
from typing import List, Dict
from multiprocessing import Pool
import json
import os
import time
import sys

class History:
    def __init__(self, file_name):
        self.file_name = file_name
        self.f = self.load_file(file_name)
        self.load_data()

    def load_file(self, file_name):
        self.f = open(file_name, 'w+')
        return self.f

    def load_data(self):
        try:
            self.history = json.loads(self.f.read())
        except Exception as e:
            print('Malformed history file, using blank')
            self.history = {}
        return self.history

    def add(self, a):
        cname = a.__class__.__name__
        if cname not in self.history:
            self.history[cname] = {a.id: a.title}
        else:
            self.history[cname][a.id] = a.title

    def contains(self, c):
        cname = a.__class__.__name__
        if cname in self.history and c.id in self.history[cname]:
            return False
        return True

    def save(self):
        self.f = self.load_file(self.file_name)
        self.f.write(json.dumps(self.history, sort_keys=True, indent=4))

class MyProgressHandler(BaseProgressHandler):
    def __init__(self):
        pass

    def initialize(self, *args, **kwargs):
        super().initialize(*args, **kwargs)

        pass

    def update(self, *args, **kwargs):
        pass

    def close(self, *args, **kwargs):
        pass

@dataclass
class Artist:
    id: int
    name: str
    title: str
    link: str
    dz: Deezer = field(repr=False)
    dict: Dict = field(init=False, repr=False)
    albums: List = field(default_factory=list, repr=False)
    total_track_count: int = field(init=False, repr=False)

    def __iter__(self):
        if not self.albums:
            self.get_albums()
        return iter(self.albums)

    def get_albums(self):
        self.dict = self.dz.get_artist(self.id)
        data = self.dz.get_artist_discography(self.id)['data']
        for album in data:
            a = [album[k] for k in ('ALB_ID', 'ALB_PICTURE',
                                    'ALB_TITLE', 'DIGITAL_RELEASE_DATE',
                                    'NUMBER_DISK', 'NUMBER_TRACK')]
            self.albums.append(Album(*a, self, album, self.dz))
        return self.albums

    def total_tracks(self):
        self.total_track_count = sum([int(a.count) for a in self.albums])
        return self.total_track_count

    def dl_album(self, a):
        return a.start_download()

    def start_download(self):
        with Pool(WORKERS) as pool:
            r = pool.map(self.dl_album, self.get_albums())
        h.save()

@dataclass
class Album:
    id: int
    picture: str
    title: str
    release_date: str
    disk: int
    count: int
    artist: Artist = field(default_factory=Artist, repr=False)
    dict: Dict = field(default_factory=dict,repr=False)
    dz: Deezer = field(default_factory=Deezer,repr=False)
    tracks: List = field(default_factory=list, repr=False)

    def __iter__(self):
        if not self.tracks:
            self.get_tracks()
        return iter(self.tracks)

    def get_tracks(self):
        self.dict = self.dz.get_album(self.id)[0]
        for track in self.dict['tracks']['data']:
            t = [track[k] for k in ('id', 'title', 'title_short', 'link',
                                    'duration')]
            self.tracks.append(Track(*t, self.artist, self, self.dz))
        return self.tracks

    def start_download(self):
        for track in self.get_tracks():
            track.start_download()
        h.add(self)
        return self

    def parent(self):
        return self.artist


@dataclass
class Track:
    id: str
    title: str
    title_short: str
    link: str
    duration: int
    artist: Artist = field(repr=False)
    album: Album = field(repr=False)
    dz: Deezer = field(repr=False)

    def start_download(self):
        try:
            track = self.dz.get_track(self.id)
            dl_dir = f'music/{self.artist.title}/'
            if int(self.album.count) > 1:
                dl_dir += f'{self.album.title}/'
            self.dz.download_track(track['info'], progress_handler=MyProgressHandler(),
                download_dir=dl_dir,
                show_messages=False)
            h.add(self)
        except Exception as e:
            pass

    def parent(self):
        return self.album

def login(arl):
    dz = Deezer()
    dz.login_via_arl(arl)
    return dz

def load_config():
    config = configparser.ConfigParser()
    config['DEFAULT'] = {'Threads': '4',
                        'DownloadList': 'yes',
                        'ConfigDirectory': 'config'}

def load_file(file_name):
    with open(file_name, 'r') as f:
        artists = [l.strip() for l in f if l]
    return artists

def search_artists(dz, artist_names, limit=-1):
    search_results = []
    search_range = artist_names if limit == -1 else artist_names[:limit]
    for artist in tqdm(search_range, desc='Searching Artists', unit=' req'):
        if limit != -1 and len(search_results) == limit:
            break
        #try:
        s = dz.search_artists(artist, limit=1)
        result = [s['data'][0][i] for i in ('id', 'name', 'name', 'link')]
        a = Artist(*result, dz)
        search_results.append(a)
        #except:
        #    print(f'Artist \'{artist}\' not found.')
    return search_results

def download_artists(artists):
    for artist in tqdm(artists, desc='Artists', leave=False):
        artist.start_download()

def main():
    dz = login('9de497677b4f4a80fdcb0b7285bb5978d0d732a288b689f3bfd228c385b1b1e'
               'd7c5ddd766817a69c61230b4d13f3cfae89999e0e674d2199bb08c97023c7a4'
               'b44a904b1a1acec41c47a9bd435d881f6e2dab81f95729e69251413ef7fc65f'
               '00b')
    artist_names = load_file('list.txt')
    artists = search_artists(dz, artist_names, limit=10)
    download_artists(artists)

if __name__ == "__main__":
    WORKERS = 8
    h = History('history.json')
    try:
        main()
    except KeyboardInterupt:
        print('Writing History...')
        h.save()
