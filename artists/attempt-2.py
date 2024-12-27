import musicbrainzngs
from typing import List, Dict
import time
from datetime import datetime
import csv

# Set up MusicBrainz API
musicbrainzngs.set_useragent(
    "AlbumRetriever",
    "1.0",
    "your-email@example.com"  # Replace with your email
)

def get_artist_id(artist_name: str) -> str:
    """Search for an artist and return their MusicBrainz ID."""
    result = musicbrainzngs.search_artists(artist=artist_name)
    if result['artist-list']:
        return result['artist-list'][0]['id']
    raise ValueError(f"Artist '{artist_name}' not found")

def get_original_albums(artist_id: str, artist_name: str) -> List[Dict]:
    """
    Retrieve original albums (excluding compilations) for an artist.
    Returns list of albums with release date and title.
    """
    # Get all releases for the artist
    result = musicbrainzngs.browse_releases(
        artist=artist_id,
        release_type=['album']  # Only get albums
    )
    
    albums = []
    for release in result['release-list']:
        # Skip compilations and live albums
        if ('secondary-type-list' in release and 
            ('compilation' in release['secondary-type-list'] or 
             'live' in release['secondary-type-list'])):
            continue
            
        # Get release date
        release_date = release.get('date', 'Unknown')
        if release_date != 'Unknown':
            try:
                # Convert date to consistent format
                date_obj = datetime.strptime(release_date, '%Y-%m-%d')
                release_date = date_obj.strftime('%Y-%m-%d')
            except ValueError:
                try:
                    # Try just the year if full date isn't available
                    date_obj = datetime.strptime(release_date, '%Y')
                    release_date = date_obj.strftime('%Y')
                except ValueError:
                    pass
        
        albums.append({
            'artist': artist_name,
            'title': release['title'],
            'date': release_date,
            'id': release['id']
        })
    
    # Sort albums by release date
    albums.sort(key=lambda x: x['date'] if x['date'] != 'Unknown' else '9999')
    return albums

def write_albums_to_csv(all_albums: List[Dict], filename: str = 'artist_albums.csv'):
    """Write albums data to CSV file."""
    if not all_albums:
        print("No albums to write to CSV.")
        return
        
    fieldnames = ['artist', 'title', 'date', 'id']
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for album in all_albums:
            writer.writerow(album)
    
    print(f"\nAlbums have been written to {filename}")

def process_artists(artists: List[str]) -> List[Dict]:
    """Process multiple artists and return their albums."""
    all_albums = []
    
    for artist in artists:
        try:
            print(f"\nRetrieving albums for {artist}...")
            artist_id = get_artist_id(artist)
            albums = get_original_albums(artist_id, artist)
            all_albums.extend(albums)
            
        except Exception as e:
            print(f"Error processing {artist}: {str(e)}")
        
        # Sleep to respect rate limiting
        time.sleep(1)
    
    return all_albums

def main():
    # Example artists
    artists = [
        "Radiohead",
        "Björk",
        "The Beatles"
    ]
    
    # Get all albums
    all_albums = process_artists(artists)
    
    # Write to CSV
    write_albums_to_csv(all_albums)

if __name__ == "__main__":
    main()