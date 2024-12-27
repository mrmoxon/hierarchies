import requests
from collections import defaultdict
from datetime import datetime
import logging
import time
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import csv
import os
from pathlib import Path
from typing import Dict, List, Set, Tuple

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RateLimiter:
    def __init__(self, min_delay: float = 1.0):
        self.last_request_time = 0
        self.min_delay = min_delay
    
    def wait_if_needed(self):
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        if time_since_last_request < self.min_delay:
            sleep_time = self.min_delay - time_since_last_request
            time.sleep(sleep_time)
        self.last_request_time = time.time()

class MusicBrainzAPI:
    def __init__(self, app_name: str, version: str, contact: str):
        self.base_url = "https://musicbrainz.org/ws/2"
        self.user_agent = f"{app_name}/{version} ( {contact} )"
        self.rate_limiter = RateLimiter(min_delay=1.1)
        
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

class MusicCatalogCollector:
    def __init__(self, app_name: str, version: str, contact: str, year_range: Tuple[int, int] = None):
        self.api = MusicBrainzAPI(app_name, version, contact)
        self.unique_songs: Dict[str, dict] = {}
        self.year_range = year_range
        
    def get_artist_id(self, artist_name: str) -> str:
        response = self.api.make_request("artist", params={"query": artist_name})
        artists = response.json().get("artists", [])
        if not artists:
            raise ValueError(f"No artist found for name: {artist_name}")
        return artists[0]["id"]
    
    def parse_year(self, date_str: str) -> Tuple[bool, int]:
        """Parse year from date string, return (success, year)"""
        if not date_str:
            return False, 0
        try:
            year = int(date_str[:4])
            return True, year
        except (ValueError, IndexError):
            return False, 0
    
    def is_valid_release(self, release: dict) -> Tuple[bool, int]:
        """Check if release meets criteria and return (is_valid, year)"""
        release_date = release.get("date", "")
        valid_year, year = self.parse_year(release_date)
        if not valid_year:
            return False, 0
            
        if self.year_range and not (self.year_range[0] <= year <= self.year_range[1]):
            return False, year
            
        release_group = release.get("release-group", {})
        secondary_types = [t.lower() for t in release_group.get("secondary-types", [])]
        
        # Only exclude live, compilation, and soundtrack releases
        if any(t in secondary_types for t in ["live", "compilation", "soundtrack"]):
            return False, year
            
        # Accept both albums and singles
        primary_type = release_group.get("primary-type", "").lower()
        if primary_type not in ["album", "single", "ep"]:
            return False, year
            
        logger.info(f"Processing {primary_type}: {release.get('title', '')}")
        return True, year

    def collect_tracks(self, artist_name: str) -> Dict[str, List[dict]]:
        """Collect all tracks and organize by year"""
        artist_id = self.get_artist_id(artist_name)
        logger.info(f"Found artist ID: {artist_id} for {artist_name}")
        
        tracks_by_year = defaultdict(list)
        total_releases = 0
        offset = 0
        limit = 100
        
        while True:
            try:
                logger.info(f"Fetching releases batch (offset: {offset})")
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
                if not releases:
                    break
                    
                total_releases += len(releases)
                logger.info(f"Processing batch of {len(releases)} releases (total: {total_releases})")
                
                for release in releases:
                    is_valid, year = self.is_valid_release(release)
                    if not is_valid:
                        continue
                        
                    release_date = release.get("date", "")
                    
                    for medium in release.get("media", []):
                        for track in medium.get("tracks", []):
                            recording = track.get("recording", {})
                            if not recording:
                                continue
                                
                            song_title = recording.get("title", track.get("title", "")).strip()
                            
                            release_group = release.get("release-group", {})
                            is_album = release_group.get("primary-type", "").lower() == "album"
                            
                            # If it's an album track, we want to keep it regardless
                            # If it's a single, only keep it if it's earlier than what we have
                            if song_title in self.unique_songs:
                                existing_track = self.unique_songs[song_title]
                                existing_is_album = existing_track.get("release_type") == "Album Track"
                                
                                # Keep the album version if either:
                                # 1. Current track is an album track and existing isn't
                                # 2. Both are album tracks but current is earlier
                                # 3. Neither are album tracks but current is earlier
                                if (is_album and not existing_is_album) or \
                                   (is_album == existing_is_album and release_date < existing_track["release_date"]):
                                    pass  # We'll replace the existing track
                                else:
                                    continue  # Keep the existing track
                                    
                            album_name = release.get("title", "") if is_album else ""
                            
                            track_data = {
                                "song_title": song_title,
                                "release_date": release_date,
                                "release_title": release.get("title", ""),
                                "release_type": "Album Track" if is_album else "Single",
                                "album_name": album_name if is_album else "",
                                "length_seconds": recording.get("length", 0) // 1000 if recording.get("length") else None
                            }
                            
                            self.unique_songs[song_title] = track_data
                            tracks_by_year[str(year)].append(track_data)
                
                if len(releases) < limit:
                    break
                    
                offset += limit
                time.sleep(1.5)  # Rate limiting between batches
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Error during batch retrieval: {str(e)}")
                time.sleep(5)
                continue
                
        logger.info(f"Processed {total_releases} total releases")
        return tracks_by_year

def save_to_csv(tracks_by_year: Dict[str, List[dict]], artist_name: str, year_range: Tuple[int, int]):
    """Save tracks to CSV file in the data directory"""
    # Create data directory if it doesn't exist
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    
    # Create sanitized filename
    artist_slug = artist_name.lower().replace(" ", "_")
    year_range_str = f"{year_range[0]}_{year_range[1]}" if year_range else "all"
    filename = data_dir / f"{artist_slug}_catalog_{year_range_str}.csv"
    
    fieldnames = [
        "year",
        "song_title",
        "release_date",
        "release_title",
        "release_type",
        "album_name",
        "length_seconds"
    ]
    
    # Read existing entries if file exists
    existing_songs = set()
    if filename.exists():
        with open(filename, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            existing_songs = {row["song_title"] for row in reader}
    
    # Append new entries
    mode = 'a' if filename.exists() else 'w'
    with open(filename, mode, newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if mode == 'w':
            writer.writeheader()
        
        new_songs = 0
        for year in sorted(tracks_by_year.keys()):
            for track in sorted(tracks_by_year[year], key=lambda x: x["release_date"]):
                if track["song_title"] not in existing_songs:
                    row = track.copy()
                    row["year"] = year
                    writer.writerow(row)
                    existing_songs.add(track["song_title"])
                    new_songs += 1
    
    logger.info(f"Added {new_songs} new songs to {filename}")
    logger.info(f"Total unique songs in file: {len(existing_songs)}")
    return filename

def collect_artist_catalog(artist_name: str, year_range: Tuple[int, int] = None):
    """Main function to collect an artist's catalog"""
    collector = MusicCatalogCollector(
        app_name="MusicCatalogCollector",
        version="1.0.0",
        contact="your.email@example.com",
        year_range=year_range
    )
    
    try:
        logger.info(f"Starting collection for {artist_name}" + 
                   (f" ({year_range[0]}-{year_range[1]})" if year_range else ""))
        
        tracks_by_year = collector.collect_tracks(artist_name)
        
        # Print summary before saving
        total_tracks = sum(len(tracks) for tracks in tracks_by_year.values())
        logger.info(f"Found {total_tracks} total unique tracks")
        
        for year in sorted(tracks_by_year.keys()):
            track_count = len(tracks_by_year[year])
            logger.info(f"Year {year}: {track_count} unique tracks")
            
        output_file = save_to_csv(tracks_by_year, artist_name, year_range)
        logger.info(f"Data saved to: {output_file}")
        
    except Exception as e:
        logger.error(f"Failed to process {artist_name}: {str(e)}")
        raise

if __name__ == "__main__":
    # Example usage
    artist_name = "The Beatles"  # Change this to any artist
    year_range = (1961, 1970)   # Optional: specify a year range, or set to None for all years
    
    collect_artist_catalog(artist_name, year_range)