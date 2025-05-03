import os
import requests
from datetime import datetime
from typing import Dict
from dotenv import load_dotenv


def send_slack_status_notification(
    sync_stats: Dict[str, int], total_events: int, success: bool = True
):
    """
    Send a status notification to Slack via webhook after iCal to Google Calendar sync.

    Args:
        sync_stats: Dictionary containing sync statistics
            - events_added: Number of events added
            - events_updated: Number of events updated
            - events_deleted: Number of events deleted
            - errors: Number of errors encountered
        total_events: Total number of events processed
        success: Boolean indicating if the operation was successful
    """
    # Load environment variables from .env file
    load_dotenv()

    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        raise ValueError("SLACK_WEBHOOK_URL environment variable not set in .env file")

    # Format the message
    status_emoji = "✅" if success else "❌"
    status_text = "Success" if success else "Failure"

    message = f"{status_emoji} *iCal to Google Calendar Sync Report*\n"
    message += f"*Date:* {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    message += f"*Status:* {status_text}\n\n"
    message += "*Sync Statistics:*\n"
    message += f"• Events Added: {sync_stats['events_added']}\n"
    message += f"• Events Updated: {sync_stats['events_updated']}\n"
    message += f"• Events Deleted: {sync_stats['events_deleted']}\n"
    message += f"• Errors Encountered: {sync_stats['errors']}\n"
    message += f"\n*Total Events Processed:* {total_events}"

    # Add error details if any errors occurred
    if sync_stats["errors"] > 0:
        message += (
            "\n\n*Note:* Some errors occurred during sync. Check the logs for details."
        )

    # Send to Slack
    payload = {"text": message}

    response = requests.post(webhook_url, json=payload)
    response.raise_for_status()
