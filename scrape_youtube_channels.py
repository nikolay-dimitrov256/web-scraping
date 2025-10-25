"""
scrape_youtube_channels.py
--------------------------

This script retrieves video data (titles, URLs, and view counts)
from specific YouTube channels using the YouTube Data API v3.

It:
1. Fetches all videos from each channel's Uploads playlist
2. Collects video statistics (view count)
3. Writes results into a CSV file per channel

Requirements:
- Python 3.7+
- requests, python-dotenv(optional)
- A valid YouTube Data API key stored in .env as API_KEY

My API_KEY is stored in a .env file so it's hidden. You can hardcode yours, if you're not going to upload the file.

Author: Nikolay Dimitrov
"""

import csv
import time
import requests
import os
from dotenv import load_dotenv

# Load environment variables from a .env file in the project root
load_dotenv()

# Read API key from environment
API_KEY = os.environ.get('API_KEY')

# YouTube API endpoints
BASE_URL = 'https://www.googleapis.com/youtube/v3/playlistItems'
VIDEO_BASE_URL = 'https://www.youtube.com/watch?v='
VIDEO_DETAILS_URL = 'https://www.googleapis.com/youtube/v3/videos'

def get_videos(playlist_id: str) -> list:
    """
    Fetch all videos from a given YouTube playlist using the Data API.

    Args:
        playlist_id (str): The ID of the YouTube playlist to fetch videos from.

    Returns:
        list[dict]: A list of dicts containing video titles, IDs, and URLs.
    """

    params = {
        'part': 'snippet',
        'playlistId': playlist_id,
        'maxResults': 50, # API max per page
        'key': API_KEY
    }

    videos = []

    # Pagination loop – YouTube API returns up to 50 items per request
    while True:
        response = requests.get(BASE_URL, params=params)

        data = response.json()

        # Extract video info (title + videoId) from response
        for item in data['items']:
            title = item['snippet']['title']
            video_id = item['snippet']['resourceId']['videoId']

            videos.append({'title': title, 'video_url': VIDEO_BASE_URL + video_id, 'video_id': video_id})

        next_page_token = data.get('nextPageToken')

        # Check for a next page token, else exit
        if next_page_token:
            params['pageToken'] = next_page_token
        else:
            break

    return videos


def get_view_counts(video_ids: list) -> dict:
    """
    Retrieve view counts for a list of YouTube video IDs.

    Args:
        video_ids (list[str]): List of video IDs.

    Returns:
        dict: A mapping of video_id → view_count.
    """

    id_string = ','.join(video_ids)
    params = {
        'part': 'statistics',
        'id': id_string,
        'key': API_KEY
    }
    response = requests.get(VIDEO_DETAILS_URL, params=params)
    data = response.json()
    stats = {}

    # Map each video ID to its view count
    for item in data['items']:
        stats[item['id']] = item['statistics']['viewCount']

    return stats


def get_video_data(playlist_id: str) -> list:
    """
    Fetch video metadata + view counts for all videos in a playlist.

    Args:
        playlist_id (str): YouTube playlist ID.

    Returns:
        list[dict]: Combined video data including title, URL, and views.
    """

    videos = get_videos(playlist_id)

    batch_size = 50
    final_data = []

    # Fetch view stats in batches of 50 videos to stay within API limits
    for i in range(0, len(videos), batch_size):
        batch = videos[i:i + batch_size]
        video_ids = [vid['video_id'] for vid in batch]
        view_counts = get_view_counts(video_ids)

        # Combine stats with video info
        for video in batch:
            views = view_counts.get(video['video_id'], '0')
            video['views'] = views
            final_data.append(video)

        time.sleep(0.4)  # avoid rate limits

    return final_data


def write_data(data: list, channel_name: str) -> None:
    """
    Save a list of video data to a CSV file named after the channel.

    Args:
        data (list[dict]): Video data (title, url, views)
        channel_name (str): Human-readable name of the channel.

    Returns:
        None
    """

    # Clean up data and ensure consistent column order
    clean_data = [
        {key: video[key] for key in ['title', 'video_url', 'views']}
        for video in data
    ]

    # Write to CSV
    with open(f'{channel_name}.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['title', 'video_url', 'views'])
        writer.writeheader()
        writer.writerows(clean_data)

    print(f'✅ CSV file saved as {channel_name}.csv')


if __name__ == '__main__':
    # Each tuple: (uploads playlist ID, channel name)
    channels = [
        ('UUbQUXtMwQM9QWQ15yDDN1BA', 'Ben Heath'),
        ('UUFF55suuW0LjzXEnzDTNCnA', 'Dara Denney'),
        ('UU3kl2OhNRZ1rH_4bnd0Ap7g', 'Professor Charley T'),
    ]

    # Fetch and save video data for each channel
    for playlist, channel_name in channels:
        video_data = get_video_data(playlist)
        write_data(video_data, channel_name)
