#!/bin/bash
echo "=== Video PTS Info ==="
ffprobe -v quiet -show_entries packet=pts_time,stream_index -select_streams v:0 -print_format csv "$1" | head -10
echo "=== Audio PTS Info ==="
ffprobe -v quiet -show_entries packet=pts_time,stream_index -select_streams a:0 -print_format csv "$1" | head -10
echo "=== Stream Info ==="
ffprobe -v quiet -show_entries stream=time_base,duration -print_format json "$1"

