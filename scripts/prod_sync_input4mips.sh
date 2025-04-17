#!/usr/bin/env bash

# Set working directory
cd /home/minxu/work/MyGit/MySrc/metadata_migrate_sync_dev/src/metadata_migrate_sync/

# Activate virtual environment
source /home/minxu/work/MyGit/MySrc/metadata_migrate_sync_dev/.venv/bin/activate


esgf15mms sync stage backup input4MIPs --prod --start-time 2025-04-09
