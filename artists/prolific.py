import requests
from collections import defaultdict
from datetime import datetime
import logging
import time
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import csv
from typing import Dict, List
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RateLimiter:
    def __init__(self):
        self.last_request_time = 0
    
    def wait_if_needed(self):
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        if time_since_last_request < 1.0:
            time.sleep(1.0 - time_since_last_request)
        self.last_request_time = time.time()

class MusicBrainzAPI:
    def __init__(self, app_name: str, version: str, contact: str):
        self.base_url = "https://musicbrainz.org/ws/2"
        self.user_agent = f"{app_name}/{version} ( {contact} )"
        self.rate_limiter = RateLimiter()
        
        retry_strategy = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        self.session = requests.Session()
        self.session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
        self.headers = {"User-Agent": self.user_agent, "Accept": "application/json"}
        
    def make_request(self, endpoint: str, params: Dict = None) -> requests.Response:
        if params is None:
            params = {}
        params["fmt"] = "json"
        self.rate_limiter.wait_if_needed()
        
        try:
            response = self.session.get(
                f"{self.base_url}/{endpoint}",
                headers=self.headers,
                params=params,
                timeout=30
            )
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for endpoint {endpoint}: {str(e)}")
            raise

class BeatlesMusicCollector:
    def __init__(self, app_name: str, version: str, contact: str):
        self.api = MusicBrainzAPI(app_name, version, contact)
        self.unique_songs = {}  # Store earliest version of each song
        
    def get_artist_id(self, artist_name: str) -> str:
        response = self.api.make_request("artist", params={"query": artist_name})
        artists = response.json().get("artists", [])
        if not artists:
            raise ValueError(f"No artist found for name: {artist_name}")
        return artists[0]["id"]
    
    def is_valid_release(self, release: dict) -> bool:
        """
        Check if the release meets our criteria:
        - Released between 1961-1970
        - Not a live recording
        - Not a compilation
        """
        # Check release date
        release_date = release.get("date", "")
        if not release_date:
            return False
        
        try:
            year = int(release_date[:4])
            if not (1961 <= year <= 1970):
                return False
        except ValueError:
            return False
            
        # Check release group attributes
        release_group = release.get("release-group", {})
        
        # Exclude live recordings and compilations
        secondary_types = [t.lower() for t in release_group.get("secondary-types", [])]
        if any(t in secondary_types for t in ["live", "compilation", "soundtrack"]):
            return False
            
        # Ensure it's a studio recording
        primary_type = release_group.get("primary-type", "").lower()
        if primary_type not in ["album", "single", "ep"]:
            return False
            
        return True

    def collect_tracks(self, artist_name: str) -> Dict[str, List[dict]]:
        """Collect unique tracks organized by year"""
        artist_id = self.get_artist_id(artist_name)
        logger.info(f"Found artist ID: {artist_id} for {artist_name}")
        
        tracks_by_year = defaultdict(list)
        offset = 0
        limit = 100
        
        while True:
            try:
                logger.info(f"Fetching releases (offset: {offset})")
                response = self.api.make_request(
                    "release",
                    params={
                        "artist": artist_id,
                        "offset": offset,
                        "limit": limit,
                        "status": "official",
                        "inc": "recordings+release-groups"
                    }
                )
                
                releases = response.json().get("releases", [])
                logger.info(f"Retrieved {len(releases)} releases")
                
                if not releases:
                    break
                
                for release in releases:
                    if not self.is_valid_release(release):
                        continue
                        
                    release_date = release.get("date", "")
                    release_year = release_date[:4]
                    
                    for medium in release.get("media", []):
                        for track in medium.get("tracks", []):
                            recording = track.get("recording", {})
                            if not recording:
                                continue
                                
                            song_title = recording.get("title", track.get("title", "")).strip()
                            
                            # Only keep the earliest version of each song
                            if song_title in self.unique_songs:
                                existing_date = self.unique_songs[song_title]["release_date"]
                                if release_date >= existing_date:
                                    continue
                            
                            track_data = {
                                "song_title": song_title,
                                "release_date": release_date,
                                "release_title": release.get("title", ""),
                                "length_seconds": recording.get("length", 0) // 1000 if recording.get("length") else None
                            }
                            
                            self.unique_songs[song_title] = track_data
                            
                if len(releases) < limit:
                    break
                    
                offset += limit
                time.sleep(0.5)
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Error during pagination at offset {offset}: {str(e)}")
                break
        
        # Organize tracks by year
        for song_data in self.unique_songs.values():
            year = song_data["release_date"][:4]
            tracks_by_year[year].append(song_data)
            
        return tracks_by_year

def append_to_csv(tracks_by_year: Dict[str, List[dict]], filename: str):
    """Append tracks to CSV file, creating it if it doesn't exist"""
    fieldnames = [
        "year",
        "song_title",
        "release_date",
        "release_title",
        "length_seconds"
    ]
    
    # Read existing entries to avoid duplicates
    existing_songs = set()
    if os.path.exists(filename):
        with open(filename, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            existing_songs = {row["song_title"] for row in reader}
    
    # Append new entries
    mode = 'a' if os.path.exists(filename) else 'w'
    with open(filename, mode, newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if mode == 'w':
            writer.writeheader()
        
        for year in sorted(tracks_by_year.keys()):
            for track in sorted(tracks_by_year[year], key=lambda x: x["release_date"]):
                if track["song_title"] not in existing_songs:
                    row = track.copy()
                    row["year"] = year
                    writer.writerow(row)
                    existing_songs.add(track["song_title"])
    
    logger.info(f"Updated {filename}")

def main():
    collector = BeatlesMusicCollector(
        app_name="BeatlesMusicCollector",
        version="1.0.0",
        contact="your.email@example.com"  # Replace with your contact info
    )
    
    filename = "beatles_catalog_1961_1970.csv"
    
    try:
        tracks_by_year = collector.collect_tracks("The Beatles")
        
        # Print summary
        for year in sorted(tracks_by_year.keys()):
            track_count = len(tracks_by_year[year])
            logger.info(f"Found {track_count} unique tracks from {year}")
            
        append_to_csv(tracks_by_year, filename)
        
    except Exception as e:
        logger.error(f"Failed to process The Beatles: {str(e)}")

if __name__ == "__main__":
    main()