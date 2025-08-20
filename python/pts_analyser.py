#!/usr/bin/env python3
"""
Audio/Video PTS Analysis Tool with Async Support
Extracts and analyzes PTS values from media files using FFmpeg
"""

import asyncio
import json
import time
import re
import sys
import os
import shutil
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any, Union
from fractions import Fraction


class PTSAnalyzer:
    def __init__(self, video_file: str):
        self.video_file = video_file
        self.real_time_start = None
        self.pts_info = {}
    
    async def run_command(self, cmd: List[str]) -> Tuple[str, str]:
        """Run a command asynchronously and return stdout, stderr"""
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        
        if process.returncode != 0:
            raise Exception(f"Command failed: {cmd}\nError: {stderr.decode()}")
            
        return stdout.decode(), stderr.decode()
    
    async def get_stream_info(self) -> Dict:
        """Get basic stream information using ffprobe asynchronously"""
        cmd = [
            'ffprobe', '-v', 'quiet', '-print_format', 'json',
            '-show_streams', '-show_format', self.video_file
        ]
        
        try:
            stdout, stderr = await self.run_command(cmd)
            return json.loads(stdout)
        except json.JSONDecodeError as e:
            raise Exception(f"Failed to parse ffprobe output: {e}")
    
    async def extract_pts_timestamps(self, stream_type: str = 'both') -> Dict[str, List[Tuple[float, float]]]:
        """
        Extract PTS values for audio and/or video streams asynchronously
        Returns dict with 'audio' and 'video' keys containing [(pts, pts_time), ...] tuples
        """
        pts_data = {'audio': [], 'video': []}
        
        # Get stream info first
        stream_info = await self.get_stream_info()
        
        # Create tasks for each stream
        tasks = []
        for stream in stream_info['streams']:
            codec_type = stream['codec_type']
            if codec_type not in ['audio', 'video'] or (stream_type != 'both' and codec_type != stream_type):
                continue
                
            tasks.append(self._extract_stream_pts(stream, codec_type, pts_data))
        
        # Run all extraction tasks concurrently
        if tasks:
            await asyncio.gather(*tasks)
        
        return pts_data
    
    async def _extract_stream_pts(self, stream: Dict, codec_type: str, pts_data: Dict) -> None:
        """Extract PTS values for a single stream asynchronously"""
        stream_index = stream['index']
        
        # Extract PTS values using ffprobe
        cmd = [
            'ffprobe', '-v', 'quiet', 
            '-select_streams', str(stream_index),
            '-show_entries', 'packet=pts_time',
            '-of', 'csv=p=0', self.video_file
        ]
        
        try:
            stdout, stderr = await self.run_command(cmd)
            lines = stdout.strip().split('\n')
            
            for line in lines:
                if line and line != 'N/A':
                    # Handle any unexpected characters
                    line = line.strip().rstrip(',')
                    try:
                        pts_time = float(line)
                        # Calculate PTS value from pts_time and time_base
                        time_base = Fraction(stream['time_base'])
                        pts = pts_time / float(time_base)
                        pts_data[codec_type].append((pts, pts_time))
                    except ValueError:
                        # Skip lines that can't be parsed as float
                        continue
                            
        except Exception as e:
            print(f"Warning: Could not extract PTS for {codec_type} stream: {e}")
    
    async def extract_pts_timestamps_alt(self, stream_type: str = 'both') -> Dict[str, List[Tuple[float, float]]]:
        """
        Alternative method to extract PTS values using a different approach asynchronously
        """
        pts_data = {'audio': [], 'video': []}
        
        # Get stream info first
        stream_info = await self.get_stream_info()
        
        # Create tasks for each stream
        tasks = []
        for stream in stream_info['streams']:
            codec_type = stream['codec_type']
            if codec_type not in ['audio', 'video'] or (stream_type != 'both' and codec_type != stream_type):
                continue
                
            tasks.append(self._extract_stream_pts_alt(stream, codec_type, pts_data))
        
        # Run all extraction tasks concurrently
        if tasks:
            await asyncio.gather(*tasks)
        
        return pts_data
    
    async def _extract_stream_pts_alt(self, stream: Dict, codec_type: str, pts_data: Dict) -> None:
        """Alternative method to extract PTS values for a single stream asynchronously"""
        stream_index = stream['index']
        
        # Use a different approach to extract PTS values
        cmd = [
            'ffprobe', '-v', 'quiet', 
            '-select_streams', str(stream_index),
            '-show_frames',
            '-show_entries', 'frame=pkt_pts_time',
            '-of', 'csv=p=0', self.video_file
        ]
        
        try:
            stdout, stderr = await self.run_command(cmd)
            lines = stdout.strip().split('\n')
            
            for line in lines:
                if line and line != 'N/A':
                    try:
                        pts_time = float(line)
                        time_base = Fraction(stream['time_base'])
                        pts = pts_time / float(time_base)
                        pts_data[codec_type].append((pts, pts_time))
                    except ValueError:
                        continue
                            
        except Exception as e:
            print(f"Warning: Could not extract PTS for {codec_type} stream: {e}")
    
    async def analyze_pts_sync(self, tolerance: float = 0.04) -> Dict:
        """
        Analyze PTS synchronization between audio and video asynchronously
        tolerance: maximum acceptable difference in seconds
        """
        # Try the alternative method if the primary one fails
        try:
            pts_data = await self.extract_pts_timestamps()
            if not pts_data['audio'] or not pts_data['video']:
                pts_data = await self.extract_pts_timestamps_alt()
        except Exception as e:
            return {"error": f"Failed to extract PTS data: {e}"}
        
        audio_pts = pts_data['audio']
        video_pts = pts_data['video']
        
        if not audio_pts or not video_pts:
            return {"error": "Missing audio or video PTS data"}
        
        sync_issues = []
        max_drift = 0
        total_drift = 0
        count = 0
        
        # Compare timestamps at similar time points
        for i, (v_pts, v_time) in enumerate(video_pts):
            # Find closest audio PTS
            closest_audio = min(audio_pts, key=lambda x: abs(x[1] - v_time))
            drift = v_time - closest_audio[1]  # Positive means audio is behind video
            
            abs_drift = abs(drift)
            total_drift += drift
            count += 1
            
            if abs_drift > max_drift:
                max_drift = abs_drift
            
            if abs_drift > tolerance:
                sync_issues.append({
                    'video_time': v_time,
                    'audio_time': closest_audio[1],
                    'drift': drift,
                    'abs_drift': abs_drift,
                    'video_pts': v_pts,
                    'audio_pts': closest_audio[0]
                })
        
        avg_drift = total_drift / count if count > 0 else 0
        
        return {
            'total_sync_issues': len(sync_issues),
            'max_drift': max_drift,
            'avg_drift': avg_drift,
            'sync_issues': sync_issues[:10],  # First 10 issues
            'audio_packets': len(audio_pts),
            'video_packets': len(video_pts)
        }
    
    async def check_pts_against_realtime(self, reference_time: Optional[datetime] = None) -> Dict:
        """
        Check PTS values against real-time clock asynchronously
        reference_time: Real-world time when video started (if known)
        """
        if reference_time is None:
            reference_time = datetime.now()
        
        # Try the alternative method if the primary one fails
        try:
            pts_data = await self.extract_pts_timestamps()
            if not pts_data['audio'] or not pts_data['video']:
                pts_data = await self.extract_pts_timestamps_alt()
        except Exception as e:
            return {"error": f"Failed to extract PTS data: {e}"}
            
        stream_info = await self.get_stream_info()
        
        # Get duration from format info
        format_info = stream_info.get('format', {})
        duration = float(format_info.get('duration', 0))
        
        analysis = {
            'reference_time': reference_time.isoformat(),
            'video_duration': duration,
            'expected_end_time': (reference_time + timedelta(seconds=duration)).isoformat(),
            'current_time': datetime.now().isoformat(),
            'streams': {}
        }
        
        for stream_type, pts_list in pts_data.items():
            if not pts_list:
                continue
                
            first_pts_time = pts_list[0][1]
            last_pts_time = pts_list[-1][1]
            
            # Calculate real-time equivalents
            real_start = reference_time + timedelta(seconds=first_pts_time)
            real_end = reference_time + timedelta(seconds=last_pts_time)
            
            analysis['streams'][stream_type] = {
                'first_pts_time': first_pts_time,
                'last_pts_time': last_pts_time,
                'real_time_start': real_start.isoformat(),
                'real_time_end': real_end.isoformat(),
                'packet_count': len(pts_list),
                'time_span': last_pts_time - first_pts_time
            }
        
        return analysis
    
    async def fix_audio_drift(self, output_file: str, drift_seconds: float = None) -> bool:
        """
        Fix audio drift by adjusting the audio PTS values asynchronously
        If drift_seconds is None, it will be calculated automatically
        """
        try:
            # Analyze sync to get drift information
            sync_info = await self.analyze_pts_sync()
            
            if 'error' in sync_info:
                print(f"Error: {sync_info['error']}")
                return False
            
            # Use provided drift or calculate average drift
            if drift_seconds is None:
                drift_seconds = sync_info['avg_drift']
                print(f"Detected average audio drift: {drift_seconds:.6f} seconds")
            
            if abs(drift_seconds) < 0.001:  # Less than 1ms drift
                print("No significant drift detected. Copying original file.")
                shutil.copy2(self.video_file, output_file)
                return True
            
            # Determine if we need to delay or advance audio
            if drift_seconds > 0:
                # Audio is behind video, need to delay audio
                print(f"Delaying audio by {drift_seconds:.6f} seconds")
                filter_complex = f"adelay={int(drift_seconds * 1000)}|{int(drift_seconds * 1000)}"
            else:
                # Audio is ahead of video, need to trim beginning
                print(f"Advancing audio by {abs(drift_seconds):.6f} seconds")
                filter_complex = f"atrim=start={abs(drift_seconds)},asetpts=PTS-STARTPTS"
            
            # Build ffmpeg command
            cmd = [
                'ffmpeg', '-i', self.video_file,
                '-filter_complex', f'[0:a]{filter_complex}[aout]',
                '-map', '0:v', '-map', '[aout]',
                '-c:v', 'copy',  # Copy video stream
                '-c:a', 'aac',   # Re-encode audio
                '-y',  # Overwrite output file
                output_file
            ]
            
            print("Fixing audio drift...")
            stdout, stderr = await self.run_command(cmd)
            
            print(f"Successfully created fixed file: {output_file}")
            return True
                
        except Exception as e:
            print(f"Error fixing audio drift: {e}")
            return False
    
    async def fix_audio_drift_alt(self, output_file: str, drift_seconds: float = None) -> bool:
        """
        Alternative method to fix audio drift using the asetpts filter asynchronously
        """
        try:
            # Analyze sync to get drift information
            sync_info = await self.analyze_pts_sync()
            
            if 'error' in sync_info:
                print(f"Error: {sync_info['error']}")
                return False
            
            # Use provided drift or calculate average drift
            if drift_seconds is None:
                drift_seconds = sync_info['avg_drift']
                print(f"Detected average audio drift: {drift_seconds:.6f} seconds")
            
            if abs(drift_seconds) < 0.001:  # Less than 1ms drift
                print("No significant drift detected. Copying original file.")
                shutil.copy2(self.video_file, output_file)
                return True
            
            # Build ffmpeg command with asetpts filter
            if drift_seconds > 0:
                # Audio is behind video, need to add offset
                filter_complex = f"asetpts=PTS+{drift_seconds}/TB"
            else:
                # Audio is ahead of video, need to subtract offset
                filter_complex = f"asetpts=PTS-{abs(drift_seconds)}/TB"
            
            cmd = [
                'ffmpeg', '-i', self.video_file,
                '-af', filter_complex,
                '-c:v', 'copy',  # Copy video stream
                '-c:a', 'aac',   # Re-encode audio
                '-y',  # Overwrite output file
                output_file
            ]
            
            print("Fixing audio drift using asetpts filter...")
            stdout, stderr = await self.run_command(cmd)
            
            print(f"Successfully created fixed file: {output_file}")
            return True
                
        except Exception as e:
            print(f"Error fixing audio drift: {e}")
            return False
    
    async def monitor_pts_realtime(self) -> None:
        """
        Monitor PTS values in real-time during playback asynchronously
        This simulates checking PTS against system clock during live processing
        """
        cmd = [
            'ffplay', '-i', self.video_file,
            '-an', '-vn',  # Disable audio and video to just analyze
            '-stats',      # Show stats
            '-autoexit'    # Exit when done
        ]
        
        print(f"Starting real-time PTS monitoring at {datetime.now()}")
        self.real_time_start = time.time()
        
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            pts_pattern = re.compile(r'pts:(\d+)\s+pts_time:([\d.]+)')
            
            # Read from stderr as ffplay outputs to stderr
            while True:
                line = (await process.stderr.readline()).decode()
                if not line and process.returncode is not None:
                    break
                    
                if 'pts_time:' in line:
                    match = pts_pattern.search(line)
                    if match:
                        pts = int(match.group(1))
                        pts_time = float(match.group(2))
                        current_real_time = time.time() - self.real_time_start
                        
                        drift = abs(pts_time - current_real_time)
                        
                        print(f"PTS: {pts_time:.3f}s | Real: {current_real_time:.3f}s | Drift: {drift:.3f}s")
                        
                        if drift > 0.1:  # Alert on significant drift
                            print(f"⚠️  SYNC WARNING: {drift:.3f}s drift detected!")
            
            await process.wait()
            
        except asyncio.CancelledError:
            print("\nMonitoring stopped by user")
            if process.returncode is None:
                process.terminate()
        except Exception as e:
            print(f"Error during monitoring: {e}")
            if process.returncode is None:
                process.terminate()
    
    async def generate_pts_report(self) -> str:
        """Generate a comprehensive PTS analysis report asynchronously"""
        try:
            # Get basic info
            stream_info = await self.get_stream_info()
            sync_analysis = await self.analyze_pts_sync()
            
            report = []
            report.append("=== PTS ANALYSIS REPORT ===\n")
            report.append(f"File: {self.video_file}")
            report.append(f"Analysis Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            
            # Stream information
            report.append("STREAM INFORMATION:")
            for stream in stream_info['streams']:
                if stream['codec_type'] in ['audio', 'video']:
                    report.append(f"  {stream['codec_type'].upper()} Stream {stream['index']}:")
                    report.append(f"    Codec: {stream.get('codec_name', 'Unknown')}")
                    report.append(f"    Time Base: {stream.get('time_base', 'Unknown')}")
                    if 'duration' in stream:
                        report.append(f"    Duration: {stream['duration']}s")
            
            # Synchronization analysis
            report.append(f"\nSYNCHRONIZATION ANALYSIS:")
            if 'error' in sync_analysis:
                report.append(f"  Error: {sync_analysis['error']}")
            else:
                report.append(f"  Audio Packets: {sync_analysis['audio_packets']}")
                report.append(f"  Video Packets: {sync_analysis['video_packets']}")
                report.append(f"  Max Drift: {sync_analysis['max_drift']:.4f}s")
                report.append(f"  Avg Drift: {sync_analysis['avg_drift']:.4f}s")
                report.append(f"  Sync Issues: {sync_analysis['total_sync_issues']}")
                
                if sync_analysis['sync_issues']:
                    report.append(f"\n  First few sync issues:")
                    for issue in sync_analysis['sync_issues'][:5]:
                        drift_dir = "audio behind" if issue['drift'] > 0 else "audio ahead"
                        report.append(f"    Time {issue['video_time']:.3f}s: {issue['abs_drift']:.4f}s drift ({drift_dir})")
            
            return '\n'.join(report)
            
        except Exception as e:
            return f"Error generating report: {e}"


async def main():
    """Main async function for command line usage"""
    if len(sys.argv) < 2:
        print("Usage: python pts_analyzer.py <command> <video_file> [options]")
        print("Commands:")
        print("  analyze <file>              - Generate PTS analysis report")
        print("  monitor <file>              - Monitor PTS in real-time")
        print("  sync <file>                 - Check audio/video sync")
        print("  fix <file> <output>         - Fix audio drift (auto-detect)")
        print("  fix <file> <output> <drift> - Fix audio drift with specified value")
        print("  fix2 <file> <output>        - Alternative fix method (auto-detect)")
        print("  fix2 <file> <output> <drift>- Alternative fix with specified value")
        sys.exit(1)
    
    # Determine command and file
    command = sys.argv[1]
    
    if command in ['analyze', 'monitor', 'sync']:
        if len(sys.argv) < 3:
            print(f"Error: {command} command requires a video file")
            sys.exit(1)
        video_file = sys.argv[2]
    elif command in ['fix', 'fix2']:
        if len(sys.argv) < 4:
            print(f"Error: {command} command requires input and output files")
            sys.exit(1)
        video_file = sys.argv[2]
        output_file = sys.argv[3]
        drift_value = float(sys.argv[4]) if len(sys.argv) > 4 else None
    else:
        print("Unknown command. Use 'analyze', 'monitor', 'sync', 'fix', or 'fix2'")
        sys.exit(1)
    
    try:
        analyzer = PTSAnalyzer(video_file)
        
        if command == 'analyze':
            report = await analyzer.generate_pts_report()
            print(report)
        elif command == 'monitor':
            await analyzer.monitor_pts_realtime()
        elif command == 'sync':
            sync_result = await analyzer.analyze_pts_sync()
            print(json.dumps(sync_result, indent=2))
        elif command == 'fix':
            success = await analyzer.fix_audio_drift(output_file, drift_value)
            if not success:
                sys.exit(1)
        elif command == 'fix2':
            success = await analyzer.fix_audio_drift_alt(output_file, drift_value)
            if not success:
                sys.exit(1)
        else:
            print("Unknown command. Use 'analyze', 'monitor', 'sync', 'fix', or 'fix2'")
            sys.exit(1)
            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
