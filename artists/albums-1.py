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
    level=logging.DEBUG,  # Changed to DEBUG for more detailed logging
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
        
        url = f"{self.base_url}/{endpoint}"
        try:
            logger.info(f"Making request to: {url}")
            logger.info(f"With parameters: {params}")
            
            response = self.session.get(
                url,
                headers=self.headers,
                params=params,
                timeout=30
            )
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for endpoint {endpoint}: {str(e)}")
            raise

class AlbumCatalogCollector:
    def __init__(self, app_name: str, version: str, contact: str, year_range: Tuple[int, int] = None):
        self.api = MusicBrainzAPI(app_name, version, contact)
        self.albums: Dict[str, dict] = {}
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
    
    def is_valid_album(self, release: dict) -> Tuple[bool, int]:
        """Check if release is a valid album and return (is_valid, year)"""
        release_date = release.get("date", "")
        valid_year, year = self.parse_year(release_date)
        
        # Debug logging
        logger.debug(f"Checking release: {release.get('title')} ({release_date})")
        
        if not valid_year:
            logger.debug(f"Invalid year format: {release_date}")
            return False, 0
            
        if self.year_range and not (self.year_range[0] <= year <= self.year_range[1]):
            logger.debug(f"Year {year} outside range {self.year_range}")
            return False, year
            
        release_group = release.get("release-group", {})
        secondary_types = [t.lower() for t in release_group.get("secondary-types", [])]
        primary_type = release_group.get("primary-type", "").lower()
        
        logger.debug(f"Release type: {primary_type}, Secondary types: {secondary_types}")
        
        # Exclude live, compilation, and soundtrack releases
        if any(t in secondary_types for t in ["live", "compilation", "soundtrack", "remix"]):
            logger.debug(f"Excluded due to secondary type: {secondary_types}")
            return False, year
            
        # Accept albums and ensure it has tracks
        if primary_type != "album":
            logger.debug(f"Not an album: {primary_type}")
            return False, year
            
        # Check if it has media/tracks
        media = release.get("media", [])
        if not media or not any(medium.get("tracks") for medium in media):
            logger.debug("No tracks found in release")
            return False, year
            
        logger.info(f"Valid album found: {release.get('title')} ({release_date})")
        return True, year

    def collect_albums(self, artist_name: str) -> Dict[str, List[dict]]:
        """Collect all albums and their track listings"""
        artist_id = self.get_artist_id(artist_name)
        logger.info(f"Found artist ID: {artist_id} for {artist_name}")
        
        albums_by_year = defaultdict(list)
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
                        "inc": "recordings+release-groups+media"
                    }
                )
                
                response_data = response.json()
                releases = response_data.get("releases", [])
                if not releases:
                    break
                
                total_releases += len(releases)
                logger.info(f"Processing batch of {len(releases)} releases (total: {total_releases})")
                logger.info(f"Release count in response: {response_data.get('release-count', 'unknown')}")
                
                # Log details of first few releases for debugging
                for idx, release in enumerate(releases[:5]):
                    logger.info(f"Release {idx + 1}:")
                    logger.info(f"  Title: {release.get('title')}")
                    logger.info(f"  Date: {release.get('date')}")
                    logger.info(f"  ID: {release.get('id')}")
                    logger.info(f"  Type: {release.get('release-group', {}).get('primary-type')}")
                    logger.info(f"  Secondary types: {release.get('release-group', {}).get('secondary-types', [])}")
                
                for release in releases:
                    is_valid, year = self.is_valid_album(release)
                    if not is_valid:
                        continue
                        
                    album_id = release["id"]
                    if album_id in self.albums:
                        continue
                        
                    tracks = []
                    for medium in release.get("media", []):
                        disc_number = medium.get("position", 1)
                        for track in medium.get("tracks", []):
                            recording = track.get("recording", {})
                            if not recording:
                                continue
                                
                            track_data = {
                                "disc_number": disc_number,
                                "track_number": track.get("position", 0),
                                "title": recording.get("title", track.get("title", "")).strip(),
                                "length_seconds": recording.get("length", 0) // 1000 if recording.get("length") else None
                            }
                            tracks.append(track_data)
                    
                    album_data = {
                        "album_id": album_id,
                        "title": release.get("title", ""),
                        "release_date": release.get("date", ""),
                        "year": year,
                        "tracks": sorted(tracks, key=lambda x: (x["disc_number"], x["track_number"]))
                    }
                    
                    self.albums[album_id] = album_data
                    albums_by_year[str(year)].append(album_data)
                
                if len(releases) < limit:
                    break
                    
                offset += limit
                time.sleep(1.5)  # Rate limiting between batches
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Error during batch retrieval: {str(e)}")
                time.sleep(5)
                continue
                
        logger.info(f"Processed {total_releases} total releases")
        return albums_by_year

def save_albums_to_csv(albums_by_year: Dict[str, List[dict]], artist_name: str, year_range: Tuple[int, int]):
    """Save albums and tracks to CSV files in the data directory"""
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    
    artist_slug = artist_name.lower().replace(" ", "_")
    year_range_str = f"{year_range[0]}_{year_range[1]}" if year_range else "all"
    
    # Save albums file
    albums_filename = data_dir / f"{artist_slug}_albums_{year_range_str}.csv"
    album_fieldnames = ["album_id", "title", "release_date", "year", "total_tracks"]
    
    # Save tracks file
    tracks_filename = data_dir / f"{artist_slug}_album_tracks_{year_range_str}.csv"
    track_fieldnames = ["album_id", "album_title", "disc_number", "track_number", "title", "length_seconds"]
    
    # Write albums
    with open(albums_filename, 'w', newline='', encoding='utf-8') as albumfile:
        album_writer = csv.DictWriter(albumfile, fieldnames=album_fieldnames)
        album_writer.writeheader()
        
        # Write tracks
        with open(tracks_filename, 'w', newline='', encoding='utf-8') as trackfile:
            track_writer = csv.DictWriter(trackfile, fieldnames=track_fieldnames)
            track_writer.writeheader()
            
            for year in sorted(albums_by_year.keys()):
                for album in sorted(albums_by_year[year], key=lambda x: x["release_date"]):
                    # Write album entry
                    album_row = {
                        "album_id": album["album_id"],
                        "title": album["title"],
                        "release_date": album["release_date"],
                        "year": album["year"],
                        "total_tracks": len(album["tracks"])
                    }
                    album_writer.writerow(album_row)
                    
                    # Write track entries
                    for track in album["tracks"]:
                        track_row = {
                            "album_id": album["album_id"],
                            "album_title": album["title"],
                            "disc_number": track["disc_number"],
                            "track_number": track["track_number"],
                            "title": track["title"],
                            "length_seconds": track["length_seconds"]
                        }
                        track_writer.writerow(track_row)
    
    logger.info(f"Album data saved to: {albums_filename}")
    logger.info(f"Track data saved to: {tracks_filename}")
    return albums_filename, tracks_filename

def collect_artist_albums(artist_name: str, year_range: Tuple[int, int] = None):
    """Main function to collect an artist's album catalog"""
    collector = AlbumCatalogCollector(
        app_name="AlbumCatalogCollector",
        version="1.0.0",
        contact="your.email@example.com",
        year_range=year_range
    )
    
    try:
        logger.info(f"Starting album collection for {artist_name}" + 
                   (f" ({year_range[0]}-{year_range[1]})" if year_range else ""))
        
        albums_by_year = collector.collect_albums(artist_name)
        
        # Print summary before saving
        total_albums = sum(len(albums) for albums in albums_by_year.values())
        total_tracks = sum(len(album["tracks"]) for year_albums in albums_by_year.values() 
                         for album in year_albums)
        
        logger.info(f"Found {total_albums} total albums with {total_tracks} tracks")
        
        for year in sorted(albums_by_year.keys()):
            albums = albums_by_year[year]
            track_count = sum(len(album["tracks"]) for album in albums)
            logger.info(f"Year {year}: {len(albums)} albums, {track_count} tracks")
            
        album_file, track_file = save_albums_to_csv(albums_by_year, artist_name, year_range)
        logger.info(f"Album data saved to: {album_file}")
        logger.info(f"Track data saved to: {track_file}")
        
    except Exception as e:
        logger.error(f"Failed to process {artist_name}: {str(e)}")
        raise

if __name__ == "__main__":
    # Example usage
    artist_name = "The Beatles"  # Change this to any artist
    year_range = (1962, 1970)   # Optional: specify a year range, or set to None for all years
    
    collect_artist_albums(artist_name, year_range)