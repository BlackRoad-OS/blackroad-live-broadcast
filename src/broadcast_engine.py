"""Live broadcast / streaming engine for BlackRoad."""

import sqlite3
import json
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional, List
from pathlib import Path
import argparse
from enum import Enum

# Database setup
DB_PATH = Path.home() / ".blackroad" / "broadcast.db"


class StreamStatus(Enum):
    """Stream status enumeration."""
    LIVE = "live"
    ENDED = "ended"
    PAUSED = "paused"


@dataclass
class Stream:
    """Stream dataclass."""
    id: str
    title: str
    category: str
    host: str
    viewers: int = 0
    started_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    status: str = StreamStatus.LIVE.value
    hls_url: str = ""
    peak_viewers: int = 0
    ended_at: Optional[str] = None
    chat_message_count: int = 0


def init_db():
    """Initialize database tables."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS streams (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            category TEXT NOT NULL,
            host TEXT NOT NULL,
            viewers INTEGER DEFAULT 0,
            peak_viewers INTEGER DEFAULT 0,
            started_at TEXT NOT NULL,
            ended_at TEXT,
            status TEXT NOT NULL,
            hls_url TEXT,
            chat_message_count INTEGER DEFAULT 0
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS viewers (
            stream_id TEXT NOT NULL,
            viewer_id TEXT NOT NULL,
            joined_at TEXT NOT NULL,
            PRIMARY KEY (stream_id, viewer_id),
            FOREIGN KEY (stream_id) REFERENCES streams(id)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS chat (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stream_id TEXT NOT NULL,
            user TEXT NOT NULL,
            message TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            FOREIGN KEY (stream_id) REFERENCES streams(id)
        )
    """)

    conn.commit()
    conn.close()


class BroadcastEngine:
    """Broadcast engine for managing live streams."""

    def __init__(self):
        """Initialize the broadcast engine."""
        init_db()

    def start_stream(self, title: str, category: str, host: str, hls_url: str = "") -> str:
        """Start a new stream.
        
        Args:
            title: Stream title
            category: Stream category
            host: Host username
            hls_url: Optional HLS URL
            
        Returns:
            Stream ID
        """
        stream_id = f"stream_{int(datetime.utcnow().timestamp())}"
        started_at = datetime.utcnow().isoformat()

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO streams (id, title, category, host, started_at, status, hls_url)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (stream_id, title, category, host, started_at, StreamStatus.LIVE.value, hls_url))
        conn.commit()
        conn.close()

        return stream_id

    def end_stream(self, stream_id: str) -> bool:
        """End a stream and record duration.
        
        Args:
            stream_id: Stream ID
            
        Returns:
            True if successful
        """
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        ended_at = datetime.utcnow().isoformat()
        cursor.execute("""
            UPDATE streams SET status = ?, ended_at = ? WHERE id = ?
        """, (StreamStatus.ENDED.value, ended_at, stream_id))
        conn.commit()
        conn.close()
        return True

    def join_stream(self, stream_id: str, viewer_id: str) -> bool:
        """Add a viewer to stream.
        
        Args:
            stream_id: Stream ID
            viewer_id: Viewer ID
            
        Returns:
            True if successful
        """
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Add viewer
        cursor.execute("""
            INSERT OR IGNORE INTO viewers (stream_id, viewer_id, joined_at)
            VALUES (?, ?, ?)
        """, (stream_id, viewer_id, datetime.utcnow().isoformat()))

        # Update viewer count
        cursor.execute("SELECT COUNT(*) FROM viewers WHERE stream_id = ?", (stream_id,))
        viewer_count = cursor.fetchone()[0]
        cursor.execute("""
            UPDATE streams SET viewers = ?, peak_viewers = MAX(peak_viewers, ?) WHERE id = ?
        """, (viewer_count, viewer_count, stream_id))

        conn.commit()
        conn.close()
        return True

    def leave_stream(self, stream_id: str, viewer_id: str) -> bool:
        """Remove a viewer from stream.
        
        Args:
            stream_id: Stream ID
            viewer_id: Viewer ID
            
        Returns:
            True if successful
        """
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("""
            DELETE FROM viewers WHERE stream_id = ? AND viewer_id = ?
        """, (stream_id, viewer_id))

        # Update viewer count
        cursor.execute("SELECT COUNT(*) FROM viewers WHERE stream_id = ?", (stream_id,))
        viewer_count = cursor.fetchone()[0]
        cursor.execute("""
            UPDATE streams SET viewers = ? WHERE id = ?
        """, (viewer_count, stream_id))

        conn.commit()
        conn.close()
        return True

    def get_live_streams(self, category: Optional[str] = None) -> List[dict]:
        """Get active streams sorted by viewers.
        
        Args:
            category: Optional category filter
            
        Returns:
            List of active stream dicts
        """
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        if category:
            cursor.execute("""
                SELECT * FROM streams
                WHERE status = ? AND category = ?
                ORDER BY viewers DESC
            """, (StreamStatus.LIVE.value, category))
        else:
            cursor.execute("""
                SELECT * FROM streams
                WHERE status = ?
                ORDER BY viewers DESC
            """, (StreamStatus.LIVE.value,))

        cols = [description[0] for description in cursor.description]
        streams = [dict(zip(cols, row)) for row in cursor.fetchall()]
        conn.close()
        return streams

    def get_stream_stats(self, stream_id: str) -> dict:
        """Get stream statistics.
        
        Args:
            stream_id: Stream ID
            
        Returns:
            Stats dict with peak_viewers, duration_min, chat_messages
        """
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT started_at, ended_at, peak_viewers, chat_message_count
            FROM streams WHERE id = ?
        """, (stream_id,))

        row = cursor.fetchone()
        conn.close()

        if not row:
            return {}

        started_at, ended_at, peak_viewers, chat_count = row
        start = datetime.fromisoformat(started_at)
        end = datetime.fromisoformat(ended_at) if ended_at else datetime.utcnow()
        duration_min = int((end - start).total_seconds() / 60)

        return {
            "peak_viewers": peak_viewers,
            "duration_min": duration_min,
            "chat_messages": chat_count,
        }

    def send_chat(self, stream_id: str, user: str, message: str) -> int:
        """Send chat message.
        
        Args:
            stream_id: Stream ID
            user: User sending message
            message: Message content
            
        Returns:
            Chat message ID
        """
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO chat (stream_id, user, message, timestamp)
            VALUES (?, ?, ?, ?)
        """, (stream_id, user, message, datetime.utcnow().isoformat()))

        chat_id = cursor.lastrowid

        # Increment chat message count
        cursor.execute("""
            UPDATE streams SET chat_message_count = chat_message_count + 1
            WHERE id = ?
        """, (stream_id,))

        conn.commit()
        conn.close()
        return chat_id

    def get_chat(self, stream_id: str, last_n: int = 50) -> List[dict]:
        """Get recent chat messages.
        
        Args:
            stream_id: Stream ID
            last_n: Number of recent messages to return
            
        Returns:
            List of chat message dicts
        """
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, user, message, timestamp FROM chat
            WHERE stream_id = ?
            ORDER BY timestamp DESC
            LIMIT ?
        """, (stream_id, last_n))

        cols = ["id", "user", "message", "timestamp"]
        messages = [dict(zip(cols, row)) for row in cursor.fetchall()]
        conn.close()
        return list(reversed(messages))


def main():
    """CLI interface for broadcast engine."""
    parser = argparse.ArgumentParser(description="BlackRoad Live Broadcast Engine")
    subparsers = parser.add_subparsers(dest="command", help="Command")

    # list command
    subparsers.add_parser("list", help="List active streams")

    # start command
    start_parser = subparsers.add_parser("start", help="Start a stream")
    start_parser.add_argument("title", help="Stream title")
    start_parser.add_argument("category", help="Stream category")
    start_parser.add_argument("host", help="Host username")
    start_parser.add_argument("--hls-url", default="", help="HLS URL")

    # chat command
    chat_parser = subparsers.add_parser("chat", help="Get stream chat")
    chat_parser.add_argument("stream_id", help="Stream ID")
    chat_parser.add_argument("--last-n", type=int, default=50, help="Last N messages")

    args = parser.parse_args()
    engine = BroadcastEngine()

    if args.command == "list":
        streams = engine.get_live_streams()
        print(json.dumps(streams, indent=2))
    elif args.command == "start":
        stream_id = engine.start_stream(args.title, args.category, args.host, args.hls_url)
        print(f"Stream started: {stream_id}")
    elif args.command == "chat":
        messages = engine.get_chat(args.stream_id, args.last_n)
        print(json.dumps(messages, indent=2))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
