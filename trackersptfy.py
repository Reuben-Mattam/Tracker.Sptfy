import time
import spotipy
import os
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
from datetime import datetime
from openpyxl import Workbook
from dotenv import load_dotenv

load_dotenv()

# API credentials from env variables
SPOTIFY_CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
SPOTIFY_CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')
SPOTIFY_REDIRECT_URI = os.getenv('SPOTIFY_REDIRECT_URI', 'http://localhost:8888/callback')

# initialise spotify api client
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id=SPOTIFY_CLIENT_ID,
    client_secret=SPOTIFY_CLIENT_SECRET,
    redirect_uri=SPOTIFY_REDIRECT_URI,
    scope="user-read-currently-playing user-read-playback-state"
))

EXCEL_FILE = "spotify_songs.xlsx"

def save_to_excel(song_data):
    """
    Save song data to Excel file with error handling, retries, and duplicate prevention
    """
    max_retries = 3
    retry_count = 0 
    
    # column mapping
    sheets = {
        "timestamp": ["Timestamp", "Track ID", "Album ID", "Artist ID", "Genres"],
        "tracks": ["Song Name", "Track ID", "Song URL", "Track Image", "Artist"],
        "albums": ["Album", "Album ID", "Album Image", "Artist"],
        "artists": ["Artist", "Artist ID", "Artist Image"],
        "genres": ["Genre", "Count"]
    }

    primary_keys = {
        "timestamp": "Timestamp",
        "tracks": "Track ID",
        "albums": "Album ID",
        "artists": "Artist ID", 
        "genres": "Genre"
    }
    
    while retry_count < max_retries:
        try:
            try:
                existing_data = pd.read_excel(EXCEL_FILE, sheet_name=None)
            except FileNotFoundError:
                existing_data = {sheet: pd.DataFrame(columns=columns) 
                    for sheet, columns in sheets.items()}
            
            can_save_any_sheet = False
            
            updated_sheets = {}
            for sheet, columns in sheets.items():
                if sheet == "genres":
                    song_genres = song_data.get("Genres", [])
                    
                    if "genres" not in existing_data:
                        genres_df = pd.DataFrame(columns=["Genre", "Count"])
                        existing_data["genres"] = genres_df
                    
                    genres_sheet = existing_data["genres"].copy()
                    for genre in song_genres:
                        # normalize genre
                        genre = genre.lower().strip()
                        
                        genre_exists = genres_sheet[genres_sheet["Genre"] == genre]
                        if not genre_exists.empty:
                            # increment count for existing genre
                            genres_sheet.loc[genres_sheet["Genre"] == genre, "Count"] += 1
                        else:
                            # add new genre
                            new_genre_row = pd.DataFrame({"Genre": [genre], "Count": [1]})
                            genres_sheet = pd.concat([genres_sheet, new_genre_row], ignore_index=True)
                    
                    updated_sheets["genres"] = genres_sheet
                    can_save_any_sheet = True
                    continue
                
                new_row = pd.DataFrame([{col: song_data[col] for col in columns}])
                

                primary_key = primary_keys[sheet]
                

                if sheet in existing_data:
                    is_duplicate = existing_data[sheet][primary_key].eq(new_row[primary_key].iloc[0]).any()
                    
                    if not is_duplicate:
                        # concatenate
                        updated_sheet = pd.concat([existing_data[sheet], new_row], ignore_index=True)
                        can_save_any_sheet = True
                    else:
                        updated_sheet = existing_data[sheet]
                else:
                    updated_sheet = new_row
                    can_save_any_sheet = True
                
                updated_sheets[sheet] = updated_sheet
            
            if can_save_any_sheet:
                with pd.ExcelWriter(EXCEL_FILE, engine="openpyxl") as writer:
                    for sheet, df in updated_sheets.items():
                        df.to_excel(writer, sheet_name=sheet, index=False)
                
                print(f"Data saved successfully to {EXCEL_FILE}!")
            else:
                print("No new unique data to save.")
            
            return
            
        except PermissionError:
            print(f"File is locked. Retry {retry_count + 1} of {max_retries}")
            time.sleep(1)
            retry_count += 1
        except Exception as e:
            print(f"Error saving to Excel: {e}")
            return

def get_currently_playing():
    """
    Fetch the currently playing track from Spotify, including genre information.
    """
    try:
        current_track = sp.current_playback()
        if current_track and current_track.get("item"):
            item = current_track["item"]
            
            # song details
            song_name = item["name"]
            track_id = item["id"]
            album_name = item["album"]["name"]
            album_id = item["album"]["id"]
            artist_name = ", ".join([artist["name"] for artist in item["artists"]])
            artist_id_list = [artist["id"] for artist in item["artists"]]
            artist_id = ", ".join(artist_id_list)
            song_url = item["external_urls"]["spotify"]
            progress_ms = current_track["progress_ms"]
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # images and genre !
            track_image = item["album"]["images"][0]["url"] if item["album"]["images"] else None
            album_image = track_image
            artist_image = None
            genres = []
            
            # fetch artist image and genres
            if artist_id_list:
                try:
                    artist_response = sp.artist(artist_id_list[0])
                    artist_image = (artist_response["images"][0]["url"] 
                                    if artist_response["images"] else None)
                    genres = artist_response.get("genres", [])
                except Exception as e:
                    print(f"Error fetching artist details: {e}")
            
            return {
                "Song Name": song_name,
                "Track ID": track_id,
                "Album": album_name,
                "Album ID": album_id,
                "Artist": artist_name,
                "Artist ID": artist_id,
                "Song URL": song_url,
                "Progress": progress_ms,
                "Timestamp": timestamp,
                "Track Image": track_image,
                "Album Image": album_image,
                "Artist Image": artist_image,
                "Genres": genres
            }
    except Exception as e:
        print(f"Error fetching current track: {e}")
    return None

def main():
    """
    Main loop to track and record currently playing songs
    """
    print("Starting Spotify song recorder. Press Ctrl+C to stop.")
    last_song = None
    last_progress = None
    
    while True:
        try:
            # get the current song
            song = get_currently_playing()
            
            if song:
                # check if new song or not
                is_new_song = last_song != song["Song Name"]
                has_restarted = (song["Progress"] is not None and 
                song["Progress"] < 2000)
                
                if is_new_song or has_restarted:
                    save_to_excel(song)
                    print(f"Recorded: {song['Song Name']} by {song['Artist']} "
                        f"at {song['Timestamp']}")
                    
                    last_song = song["Song Name"]
                    last_progress = song["Progress"]
            
            time.sleep(10)
            
        except KeyboardInterrupt:
            print("\nStopping Spotify song recorder.")
            break
        except Exception as e:
            print(f"Unexpected error: {e}")
            time.sleep(30)

if __name__ == "__main__":
    main()