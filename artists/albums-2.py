import requests
import json
import csv
from pathlib import Path
import time
from typing import Dict, List
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MusicBrainzClient:
    def __init__(self):
        self.base_url = "https://musicbrainz.org/ws/2"
        self.headers = {
            "User-Agent": "BeatlesDataCollector/1.0 (your@email.com)",
            "Accept": "application/json"
        }
    
    def _make_request(self, endpoint: str, params: Dict = None) -> Dict:
        """Make a request to MusicBrainz API with rate limiting"""
        if params is None:
            params = {}
        params["fmt"] = "json"
        
        url = f"{self.base_url}/{endpoint}"
        logger.info(f"Requesting: {url} with params {params}")
        
        # Rate limiting
        time.sleep(1)
        
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()

    def get_all_releases(self, artist_id: str) -> List[Dict]:
        """Get all releases for an artist with pagination"""
        all_releases = []
        offset = 0
        limit = 100
        
        while True:
            params = {
                "artist": artist_id,
                "inc": "recordings+release-groups+media",
                "limit": limit,
                "offset": offset
            }
            
            data = self._make_request("release", params)
            releases = data.get("releases", [])
            
            if not releases:
                break
                
            logger.info(f"Retrieved {len(releases)} releases (offset: {offset})")
            all_releases.extend(releases)
            
            if len(releases) < limit:
                break
                
            offset += limit
            
        return all_releases

def save_raw_data(data: Dict, filename: str):
    """Save raw JSON data"""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def process_releases(releases: List[Dict]) -> tuple[List[Dict], List[Dict]]:
    """Process releases into albums and songs"""
    albums = []
    songs = []
    
    for release in releases:
        # Basic release info
        release_info = {
            "id": release.get("id"),
            "title": release.get("title"),
            "date": release.get("date", ""),
            "status": release.get("status", ""),
            "packaging": release.get("packaging", ""),
        }
        
        # Release group info
        release_group = release.get("release-group", {})
        release_info.update({
            "type": release_group.get("primary-type", ""),
            "secondary_types": release_group.get("secondary-types", []),
        })
        
        # Track info
        tracks = []
        for medium in release.get("media", []):
            disc_number = medium.get("position", 1)
            for track in medium.get("tracks", []):
                recording = track.get("recording", {})
                track_info = {
                    "release_id": release["id"],
                    "release_title": release["title"],
                    "disc_number": disc_number,
                    "position": track.get("position", 0),
                    "title": recording.get("title", track.get("title", "")),
                    "length": recording.get("length"),
                    "id": recording.get("id")
                }
                tracks.append(track_info)
        
        release_info["track_count"] = len(tracks)
        albums.append(release_info)
        songs.extend(tracks)
    
    return albums, songs

def save_to_csv(data: List[Dict], filename: str):
    """Save data to CSV file"""
    if not data:
        return
        
    fieldnames = list(data[0].keys())
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

def main():
    # The Beatles' MusicBrainz ID
    beatles_id = "b10bbbfc-cf9e-42e0-be17-e2c3e1d2600d"
    
    client = MusicBrainzClient()
    
    # Create output directory
    output_dir = Path("beatles_data")
    output_dir.mkdir(exist_ok=True)
    
    # Get all releases
    logger.info("Fetching all Beatles releases...")
    releases = client.get_all_releases(beatles_id)
    logger.info(f"Found {len(releases)} total releases")
    
    # Save raw data
    save_raw_data({"releases": releases}, output_dir / "raw_data.json")
    logger.info("Saved raw data")
    
    # Process into albums and songs
    albums, songs = process_releases(releases)
    
    # Save processed data
    save_to_csv(albums, output_dir / "albums.csv")
    save_to_csv(songs, output_dir / "songs.csv")
    
    # Print summary
    logger.info(f"\nSummary:")
    logger.info(f"Total releases found: {len(releases)}")
    logger.info(f"Total albums processed: {len(albums)}")
    logger.info(f"Total songs processed: {len(songs)}")
    
    # Print some example data
    logger.info("\nExample Albums:")
    for album in sorted(albums[:5], key=lambda x: x.get("date", "")):
        logger.info(f"{album['date']} - {album['title']} ({album['type']}) - {album['track_count']} tracks")

if __name__ == "__main__":
    main()