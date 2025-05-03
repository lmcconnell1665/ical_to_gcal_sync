#!/bin/bash
TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
echo "Starting schedule sync at $TIMESTAMP"

cd /home/luke/repos/ical_to_gcal_sync

# Activate virtual environment
source .venv/bin/activate
if [ $? -ne 0 ]; then
  echo "Failed to activate virtual environment. Exiting."
  exit 1
fi
echo "Virtual environment activated."

# Run the Python script and log output
echo "Running python3 ical_to_gcal_sync.py..."
python3 ical_to_gcal_sync.py
EXIT_CODE=$?

# Check exit code and log result
END_TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
if [ $EXIT_CODE -eq 0 ]; then
  echo "Python script completed successfully."
  echo "Schedule sync finished successfully at $END_TIMESTAMP"
  exit 0
else
  echo "Python script failed with exit code $EXIT_CODE. Check $LOG_FILE for details."
  echo "Schedule sync finished with errors at $END_TIMESTAMP"
  exit $EXIT_CODE
fi