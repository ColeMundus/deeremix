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
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

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
            self.history = json.load(self.f)
        except Exception as e:
            print(e)
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
        cname = c.__class__.__name__
        return cname in self.history and c.id in self.history[cname]

    def save(self):
        self.f = self.load_file(self.file_name)
        self.f.write(json.dumps(self.history, sort_keys=True, indent=4))

    def search_artist_str(self, artist):
        return 'Artist' in self.history and artist in self.history['Artist'].values()

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
        if not self.albums:
            self.dict = self.dz.get_artist(self.id)
            data = self.dz.get_artist_discography(self.id)['data']
            for album in data:
                a = [album[k] for k in ('ALB_ID', 'ALB_PICTURE',
                                        'ALB_TITLE', 'DIGITAL_RELEASE_DATE',
                                        'NUMBER_DISK', 'NUMBER_TRACK')]
                self.albums.append(Album(*a, self, album, self.dz))
        return self.albums

    def total_tracks(self):
        if not self.albums:
            self.get_albums()
        self.total_track_count = sum([int(a.count) for a in self.albums])
        return self.total_track_count

    def dl_album(self, a):
        return a.start_download()

    def start_download(self):
        download_queue = [album.start_download() for album in self]
        return download_queue
        """with Pool(WORKERS) as pool:
            download_queue = []
            for album in self.get_albums():
                if h.contains(album):
                    print(f'Album \'{album.name}\': already downloaded')
                    continue
                download_queue.append(album)
            results = pool.map(self.dl_album, download_queue)
            for r in results:
                h.add(r)
        print(f'Finished download of Artist: {self.name} (failed: )')
        h.add(self)
        h.save()"""

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
        if not self.tracks:
            self.dict = self.dz.get_album(self.id)[0]
            for track in self.dict['tracks']['data']:
                t = [track[k] for k in ('id', 'title', 'title_short', 'link',
                                        'duration')]
                self.tracks.append(Track(*t, self.artist, self, self.dz))
        return self.tracks

    def start_download(self):
        download_queue = [track.start_download for track in self.get_tracks()]
        return download_queue

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
            re = lambda s: s.replace('/','\/')
            dl_dir = f"{args.download_directory}/{re(self.artist.title)}/"
            if int(self.album.count) > 1:
                dl_dir += f"{re(self.album.title)}/"
            self.dz.download_track(track['info'], progress_handler=MyProgressHandler(),
                download_dir=dl_dir, quality=track_formats.MP3_320,
                show_messages=False)
            return self, dl_dir
        except Exception as e:
            if isinstance(e, KeyboardInterrupt):
                sys.exit()
            return 0, 0

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
        if h.search_artist_str(artist):
            print(f'Artist \'{artist}\': already downloaded')
            continue
        if limit != -1 and len(search_results) == limit:
            break
        try:
            s = dz.search_artists(artist, limit=1)
            result = [s['data'][0][i] for i in ('id', 'name', 'name', 'link')]
            a = Artist(*result, dz)
            search_results.append(a)
        except Exception as e:
            if isinstance(e, KeyboardInterrupt):
                sys.exit()
            print(f'Artist \'{artist}\': not found.')
    return search_results

def download_artists(artists):
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            for artist in artists:
                total_size = 0
                failed = 0
                with tqdm(total=artist.total_tracks(), desc=artist.name, unit=' tracks') as pbar:
                    #futures = [executor.submit(track.start_download) for album in track for track in album]
                    future_dl = {executor.submit(track.start_download): track.id for album in artist for track in album}
                    for future in as_completed(future_dl):
                        track_id = future_dl[future]
                        try:
                            t, dl_dir = future.result(timeout=60)
                            if t:
                                 total_size += folder_size(dl_dir)
                            else:
                                failed += 1
                        except Excetion as e:
                            print(f"Exception on track id: {track_id}, {e}")
                        pbar.update(1)
                print(f'Finished: [Artist: {artist.name}]'
                      f'[Album Count: {len(artist.albums)}]'
                      f'[Total Size: {human_readable_size(total_size)}]')

def folder_size(path):
    total = 0
    for entry in os.scandir(path):
        if entry.is_file():
            total += entry.stat().st_size
        elif entry.is_dir():
            total += folder_size(entry.path)
    return total

def human_readable_size(size, decimal_places=2):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
        if size < 1000.0 or unit == 'PB':
            break
        size /= 1000.0
    return f"{size:.{decimal_places}f} {unit}"

def parse_args():
    parser = argparse.ArgumentParser(description='Deeremix')
    parser.add_argument('-a', '--arl', type=str,
                        help='\'arl\' authentication string required for '
                         'downloads, see readme for how to obtain')
    parser.add_argument('-w', '--workers', default=10, type=int,
                        help='Numbemr of workers (processers) to use for '
                         'download threads (default: 8)')
    parser.add_argument('--history', default='history.json', type=str,
                        help='Path to history file, stores completed download '
                         'rules (default: \'history.json\')')
    parser.add_argument('-l', '--limit', default=-1, type=int,
                        help='Limit the number of artists to download '
                         '(unlimited: -1, default: -1)')
    parser.add_argument('-d', '--download-directory', dest='download_directory',
                        type=str, default='music/', help='Default download '
                        'directory destination (default: \'music/\')')
    args = parser.parse_args()
    return args

def main(args):
    dz = login('9de497677b4f4a80fdcb0b7285bb5978d0d732a288b689f3bfd228c385b1b1e'
               'd7c5ddd766817a69c61230b4d13f3cfae89999e0e674d2199bb08c97023c7a4'
               'b44a904b1a1acec41c47a9bd435d881f6e2dab81f95729e69251413ef7fc65f'
               '00b')
    artist_names = load_file('list.txt')
    artists = search_artists(dz, artist_names, limit=args.limit)
    download_artists(artists)

if __name__ == "__main__":
    args = parse_args()
    WORKERS = args.workers
    h = History('history.json')
    try:
        main(args)
    except KeyboardInterrupt:
        print('Writing History...')
        h.save()
