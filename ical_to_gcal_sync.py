from __future__ import print_function

import logging
import time
import string
import re
import sys
import os

import googleapiclient
import arrow
from googleapiclient import errors
from icalevents.icalevents import events
from icalevents.icalparser import Event
from dateutil.tz import gettz

from datetime import datetime, timezone, timedelta

from auth import auth_with_calendar_api
from pathlib import Path
from httplib2 import Http
from slack import send_slack_status_notification

config = {}
config_path = os.environ.get("CONFIG_PATH", "config.py")
exec(Path(config_path).read_text(), config)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if config.get("LOGFILE", None):
    handler = logging.FileHandler(filename=config["LOGFILE"], mode="a")
else:
    handler = logging.StreamHandler(sys.stderr)
handler.setFormatter(logging.Formatter("%(asctime)s|[%(levelname)s] %(message)s"))
logger.addHandler(handler)

DEFAULT_TIMEDELTA = timedelta(days=365)


def get_current_events_from_files(path):
    """Retrieves data from iCal files.  Assumes that the files are all
    *.ics files located in a single directory.

    Returns the parsed list of events or None if an error occurs.
    """

    from glob import glob
    from os.path import join

    event_ics = glob(join(path, "*.ics"))

    if len(event_ics) > 0:
        ics = event_ics[0]
        cal = get_current_events(feed_url_or_path=ics, files=True)
        for ics in event_ics[1:]:
            evt = get_current_events(feed_url_or_path=ics, files=True)
            if len(evt) > 0:
                cal.extend(evt)
        return cal
    else:
        return None


def get_current_events(feed_url_or_path, files):
    """Retrieves data from an iCal feed and returns a list of icalevents
    Event objects

    Returns the parsed list of events or None if an error occurs.
    """

    events_end = datetime.now()
    if config.get("ICAL_DAYS_TO_SYNC", 0) == 0:
        # default to 1 year ahead
        events_end += DEFAULT_TIMEDELTA
    else:
        # add on a number of days
        events_end += timedelta(days=config["ICAL_DAYS_TO_SYNC"])

    try:
        if files:
            cal = events(file=feed_url_or_path, end=events_end)
        else:
            # Use requests for better basic auth handling
            import requests
            from requests.auth import HTTPBasicAuth
            import tempfile
            import os

            session = requests.Session()
            if config.get("ICAL_FEED_USER") and config.get("ICAL_FEED_PASS"):
                session.auth = HTTPBasicAuth(
                    config.get("ICAL_FEED_USER"), config.get("ICAL_FEED_PASS")
                )

            # Add headers
            headers = {"Accept": "application/json", "Content-Type": "application/json"}

            # Make the request
            response = session.get(
                feed_url_or_path,
                headers=headers,
                verify=config.get("ICAL_FEED_VERIFY_SSL_CERT", True),
            )
            response.raise_for_status()

            # Save response to temporary file
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".ics", delete=False
            ) as temp_file:
                temp_file.write(response.text)
                temp_path = temp_file.name

            try:
                # Use the temporary file with icalevents
                cal = events(
                    file=temp_path,
                    start=datetime.now()
                    - timedelta(days=config.get("PAST_DAYS_TO_SYNC", 0)),
                    end=events_end,
                )
            finally:
                # Clean up the temporary file
                os.unlink(temp_path)
    except Exception as e:
        logger.error("> Error retrieving iCal data ({})".format(e))
        return None

    # Process multi-day events
    processed_events = []
    for ev in cal:
        # Handle timezone
        if ev.start.tzinfo is None:
            ev.start = ev.start.replace(tzinfo=timezone.utc)
        if ev.end is not None and ev.end.tzinfo is None:
            ev.end = ev.end.replace(tzinfo=timezone.utc)

        # Try to parse multi-day events
        day_events = parse_multi_day_event(ev)
        if day_events:
            logger.debug(
                f"Splitting multi-day event {ev.summary} into {len(day_events)} day events"
            )
            processed_events.extend(day_events)
        else:
            processed_events.append(ev)

    logger.debug(f"Processed {len(processed_events)} events total")
    return processed_events


def get_gcal_events(calendar_id, service, from_time=None):
    """Retrieves the current set of Google Calendar events from the selected
    user calendar. If from_time is not specified, includes events from all-time.

    Returns a dict containing the event(s) existing in the calendar.
    """

    # The list() method returns a dict containing various metadata along with the actual calendar entries (if any).
    # It is not guaranteed to return all available events in a single call, and so may need called multiple times
    # until it indicates no more events are available, signalled by the absence of "nextPageToken" in the result dict

    # make an initial call, if this returns all events we don't need to do anything else,,,
    events_result = (
        service.events()
        .list(
            calendarId=calendar_id,
            timeMin=from_time,
            singleEvents=True,
            orderBy="startTime",
            showDeleted=True,
        )
        .execute()
    )

    events = events_result.get("items", [])
    # if nextPageToken is NOT in the dict, this should be everything
    if "nextPageToken" not in events_result:
        logger.info(
            "> Found {:d} upcoming events in Google Calendar (single page)".format(
                len(events)
            )
        )
        return events

    # otherwise keep calling the method, passing back the nextPageToken each time
    while "nextPageToken" in events_result:
        token = events_result["nextPageToken"]
        events_result = (
            service.events()
            .list(
                calendarId=calendar_id,
                timeMin=from_time,
                pageToken=token,
                singleEvents=True,
                orderBy="startTime",
                showDeleted=True,
            )
            .execute()
        )
        newevents = events_result.get("items", [])
        events.extend(newevents)

    logger.info(
        "> Found {:d} upcoming events in Google Calendar (multi page)".format(
            len(events)
        )
    )
    return events


def get_gcal_datetime(py_datetime, gcal_timezone):
    py_datetime = py_datetime.astimezone(gettz(gcal_timezone))
    return {
        "dateTime": py_datetime.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "timeZone": gcal_timezone,
    }


def get_gcal_date(py_datetime):
    return {"date": py_datetime.strftime("%Y-%m-%d")}


def create_id(uid, begintime, endtime, prefix=""):
    """Converts ical UUID, begin and endtime to a valid Gcal ID

    Characters allowed in the ID are those used in base32hex encoding, i.e. lowercase letters a-v and digits 0-9, see section 3.1.2 in RFC2938
    Te length of the ID must be between 5 and 1024 characters
    https://developers.google.com/resources/api-libraries/documentation/calendar/v3/python/latest/calendar_v3.events.html

    Returns:
        ID
    """
    allowed_chars = string.ascii_lowercase[:22] + string.digits
    return (
        prefix
        + re.sub("[^{}]".format(allowed_chars), "", uid.lower())
        + str(arrow.get(begintime).int_timestamp)
        + str(arrow.get(endtime).int_timestamp)
    )


def parse_multi_day_event(ev: Event) -> list[Event]:
    """Parse a multi-day event and split it into individual day events.

    Args:
        ev: The original multi-day event

    Returns:
        A list of individual day events, or None if the event couldn't be parsed
    """
    if not ev.end or (ev.end - ev.start).days <= 0:
        return None

    if not ev.description:
        return None

    # Extract DPXX Report sections
    dp_pattern = r"\* DP(\d+) Report: (\w+) (\d+):(\d+) Release: (\w+) (\d+):(\d+)(.*?)(?=\* DP\d+ Report:|$)"
    dp_sections = re.findall(dp_pattern, ev.description, re.DOTALL)

    if not dp_sections:
        return None

    day_events = []
    # Create a mapping of month abbreviations to month numbers
    month_map = {
        "Jan": 1,
        "Feb": 2,
        "Mar": 3,
        "Apr": 4,
        "May": 5,
        "Jun": 6,
        "Jul": 7,
        "Aug": 8,
        "Sep": 9,
        "Oct": 10,
        "Nov": 11,
        "Dec": 12,
    }

    for (
        dp_num,
        dep_airport,
        dep_hour,
        dep_min,
        arr_airport,
        arr_hour,
        arr_min,
        section_text,
    ) in dp_sections:
        # Extract flights from this section
        flight_pattern = r"Flight Number: UA (\d+)\s+(\w+)\s+(\w+)\s+(\d+):(\d+)\s+(\w+)\s+(\w+)\s+(\d+):(\d+)"
        flights = re.findall(flight_pattern, section_text)

        if not flights:
            continue

        # Extract the date from the first flight's departure
        first_flight = flights[0]
        month_abbr = first_flight[2][:3]  # Get first 3 chars for month abbreviation
        day = int(first_flight[2][3:])  # Get the day number
        month = month_map[month_abbr]

        # Get the year from the event's start date
        year = ev.start.year

        # Create the date for this day's events
        current_date = datetime(year, month, day).date()

        # Create a list of unique airports in order for this day's flights
        day_airports = []
        last_airport = None

        for flight in flights:
            # Add departure airport if it's different from the last one
            if flight[1] != last_airport:
                day_airports.append(flight[1])
                last_airport = flight[1]
            # Add arrival airport if it's different from the last one
            if flight[5] != last_airport:
                day_airports.append(flight[5])
                last_airport = flight[5]

        if day_airports:
            # Create a new event for this day
            day_event = Event()
            day_event.uid = f"{ev.uid}_DP{dp_num}"
            day_event.start = datetime.combine(current_date, datetime.min.time())
            day_event.end = datetime.combine(
                current_date, datetime.min.time()
            ) + timedelta(days=1)

            # Set title with airports for this day's flights
            day_event.summary = " → ".join(day_airports)
            day_event.description = ev.description
            day_events.append(day_event)

    return day_events


if __name__ == "__main__":
    mandatory_configs = ["CREDENTIAL_PATH", "ICAL_FEEDS", "APPLICATION_NAME"]
    for mandatory in mandatory_configs:
        if not config.get(mandatory) or config[mandatory][0] == "<":
            logger.error(
                "Must specify a non-blank value for %s in the config file" % mandatory
            )
            sys.exit(1)

    # setting up Google Calendar API for use
    logger.debug("> Loading credentials")
    service = auth_with_calendar_api(config)

    # dateime instance representing the start of the current day (UTC)
    today = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    for feed in config.get("ICAL_FEEDS"):
        logger.info("> Processing source %s" % feed["source"])

        # retrieve events from Google Calendar, starting from beginning of current day
        logger.info("> Retrieving events from Google Calendar")
        gcal_events = get_gcal_events(
            calendar_id=feed["destination"],
            service=service,
            from_time=(
                today - timedelta(days=config.get("PAST_DAYS_TO_SYNC", 0))
            ).isoformat(),
        )

        # retrieve events from the iCal feed
        if feed["files"]:
            logger.info("> Retrieving events from local folder")
            ical_cal = get_current_events_from_files(feed["source"])
        else:
            logger.info("> Retrieving events from iCal feed")
            ical_cal = get_current_events(feed_url_or_path=feed["source"], files=False)

        if ical_cal is None:
            sys.exit(-1)

        # convert iCal event list into a dict indexed by (converted) iCal UID
        ical_events = {}

        for ev in ical_cal:
            try:
                if "EVENT_PREPROCESSOR" in config:
                    keep = config["EVENT_PREPROCESSOR"](ev)
                    if not keep:
                        logger.debug(
                            "Skipping event %s - EVENT_PREPROCESSOR returned false"
                            % (str(ev))
                        )
                        continue

            except Exception as ex:
                logger.error(
                    "Error processing entry (%s, %s) - leaving as-is"
                    % (str(ev), str(ex))
                )

            ical_events[
                create_id(ev.uid, ev.start, ev.end, config.get("EVENT_ID_PREFIX", ""))
            ] = ev

        logger.debug("> Collected {:d} iCal events".format(len(ical_events)))

        # retrieve the Google Calendar object itself
        gcal_cal = service.calendars().get(calendarId=feed["destination"]).execute()

        logger.info("> Processing Google Calendar events...")
        gcal_event_ids = [ev["id"] for ev in gcal_events]

        # Track sync statistics
        sync_stats = {
            "events_added": 0,
            "events_updated": 0,
            "events_deleted": 0,
            "errors": 0,
        }

        # first check the set of Google Calendar events against the list of iCal
        # events. Any events in Google Calendar that are no longer in iCal feed
        # get deleted. Any events still present but with changed start/end times
        # get updated.
        for gcal_event in gcal_events:
            eid = gcal_event["id"]

            if eid not in ical_events:
                # if a gcal event has been deleted from iCal, also delete it from gcal.
                try:
                    # already marked as deleted, so it's in the "trash" or "bin"
                    if gcal_event["status"] == "cancelled":
                        continue

                    logger.info(
                        '> Deleting event "{}" from Google Calendar...'.format(
                            gcal_event.get("summary", "<unnamed event>")
                        )
                    )
                    service.events().delete(
                        calendarId=feed["destination"], eventId=eid
                    ).execute()
                    sync_stats["events_deleted"] += 1
                    time.sleep(config["API_SLEEP_TIME"])
                except googleapiclient.errors.HttpError:
                    pass  # event already marked as deleted
            else:
                ical_event = ical_events[eid]
                gcal_begin = arrow.get(
                    gcal_event["start"].get("dateTime", gcal_event["start"].get("date"))
                )
                gcal_end = arrow.get(
                    gcal_event["end"].get("dateTime", gcal_event["end"].get("date"))
                )

                gcal_has_location = bool(gcal_event.get("location"))
                ical_has_location = bool(ical_event.location)

                gcal_has_description = "description" in gcal_event
                ical_has_description = ical_event.description is not None

                # event name can be left unset, in which case there's no summary field
                gcal_name = gcal_event.get("summary", None)
                log_name = "<unnamed event>" if gcal_name is None else gcal_name

                times_differ = (
                    gcal_begin != ical_event.start or gcal_end != ical_event.end
                )
                titles_differ = gcal_name != ical_event.summary
                locs_differ = (
                    gcal_has_location != ical_has_location
                    and gcal_event.get("location") != ical_event.location
                )
                descs_differ = gcal_has_description != ical_has_description and (
                    gcal_event.get("description") != ical_event.description
                )

                needs_undelete = (
                    config.get("RESTORE_DELETED_EVENTS", False)
                    and gcal_event["status"] == "cancelled"
                )

                changes = []
                if times_differ:
                    changes.append("start/end times")
                if titles_differ:
                    changes.append("titles")
                if locs_differ:
                    changes.append("locations")
                if descs_differ:
                    changes.append("descriptions")
                if needs_undelete:
                    changes.append("undeleted")

                # check if the iCal event has a different: start/end time, name, location,
                # or description, and if so sync the changes to the GCal event
                if (
                    needs_undelete
                    or times_differ
                    or titles_differ
                    or locs_differ
                    or descs_differ
                ):
                    logger.debug(
                        '> Updating event "{}" due to changes: {}'.format(
                            log_name, ", ".join(changes)
                        )
                    )
                    delta = ical_event.end - ical_event.start
                    # all-day events handled slightly differently
                    # TODO multi-day events?
                    if delta.days >= 1:
                        gcal_event["start"] = get_gcal_date(ical_event.start)
                        gcal_event["end"] = get_gcal_date(ical_event.end)
                    else:
                        gcal_event["start"] = get_gcal_datetime(
                            ical_event.start, gcal_cal["timeZone"]
                        )
                        if ical_event.end is not None:
                            gcal_event["end"] = get_gcal_datetime(
                                ical_event.end, gcal_cal["timeZone"]
                            )

                    # if the event was deleted, the status will be 'cancelled' - this restores it
                    gcal_event["status"] = "confirmed"

                    gcal_event["summary"] = ical_event.summary
                    gcal_event["description"] = ical_event.description
                    if feed["files"]:
                        url_feed = "https://events.from.ics.files.com"
                    else:
                        url_feed = feed["source"]
                    gcal_event["source"] = {
                        "title": "Imported from ical_to_gcal_sync.py",
                        "url": url_feed,
                    }
                    gcal_event["location"] = ical_event.location

                    service.events().update(
                        calendarId=feed["destination"], eventId=eid, body=gcal_event
                    ).execute()
                    sync_stats["events_updated"] += 1
                    time.sleep(config["API_SLEEP_TIME"])

        # now add any iCal events not already in the Google Calendar
        logger.info("> Processing iCal events...")
        for ical_id, ical_event in ical_events.items():
            if ical_id not in gcal_event_ids:
                gcal_event = {}
                gcal_event["summary"] = ical_event.summary
                gcal_event["id"] = ical_id
                gcal_event["description"] = ical_event.description
                if feed["files"]:
                    url_feed = "https://events.from.ics.files.com"
                else:
                    url_feed = feed["source"]
                gcal_event["source"] = {
                    "title": "Imported from ical_to_gcal_sync.py",
                    "url": url_feed,
                }
                gcal_event["location"] = ical_event.location

                # check if no time specified in iCal, treat as all day event if so
                delta = ical_event.end - ical_event.start
                # TODO multi-day events?
                if delta.days >= 1:
                    gcal_event["start"] = get_gcal_date(ical_event.start)
                    logger.info(
                        "iCal all-day event {} to be added at {}".format(
                            ical_event.summary, ical_event.start
                        )
                    )
                    if ical_event.end is not None:
                        gcal_event["end"] = get_gcal_date(ical_event.end)
                else:
                    gcal_event["start"] = get_gcal_datetime(
                        ical_event.start, gcal_cal["timeZone"]
                    )
                    logger.info(
                        "iCal event {} to be added at {}".format(
                            ical_event.summary, ical_event.start
                        )
                    )
                    if ical_event.end is not None:
                        gcal_event["end"] = get_gcal_datetime(
                            ical_event.end, gcal_cal["timeZone"]
                        )

                logger.info(
                    'Adding iCal event called "{}", starting {}'.format(
                        ical_event.summary, gcal_event["start"]
                    )
                )

                try:
                    time.sleep(config["API_SLEEP_TIME"])
                    service.events().insert(
                        calendarId=feed["destination"], body=gcal_event
                    ).execute()
                    sync_stats["events_added"] += 1
                except Exception as ei:
                    logger.warning(
                        "Error inserting: %s (%s)" % (gcal_event["id"], str(ei))
                    )
                    sync_stats["errors"] += 1
                    time.sleep(config["API_SLEEP_TIME"])
                    try:
                        service.events().update(
                            calendarId=feed["destination"],
                            eventId=gcal_event["id"],
                            body=gcal_event,
                        ).execute()
                        sync_stats["events_updated"] += 1
                    except Exception as ex:
                        logger.error("Error updating: %s (%s)" % (gcal_event["id"], ex))
                        sync_stats["errors"] += 1

        logger.info("> Processing of source %s completed" % feed["source"])

        # Send Slack notification with sync statistics
        try:
            total_events = (
                sync_stats["events_added"]
                + sync_stats["events_updated"]
                + sync_stats["events_deleted"]
            )
            success = sync_stats["errors"] == 0
            send_slack_status_notification(sync_stats, total_events, success)
        except Exception as e:
            logger.error(f"Failed to send Slack notification: {str(e)}")
