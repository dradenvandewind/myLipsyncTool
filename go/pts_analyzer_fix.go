package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// PTSAnalyzer represents the main analyzer struct
type PTSAnalyzer struct {
	VideoFile     string
	RealTimeStart time.Time
	PTSInfo       map[string]interface{}
}

// StreamInfo represents stream information from ffprobe
type StreamInfo struct {
	Streams []Stream `json:"streams"`
	Format  Format   `json:"format"`
}

// Stream represents a single stream
type Stream struct {
	Index     int    `json:"index"`
	CodecType string `json:"codec_type"`
	CodecName string `json:"codec_name"`
	TimeBase  string `json:"time_base"`
	Duration  string `json:"duration"`
}

// Format represents format information
type Format struct {
	Duration string `json:"duration"`
}

// SyncIssue represents a synchronization issue
type SyncIssue struct {
	VideoTime float64 `json:"video_time"`
	AudioTime float64 `json:"audio_time"`
	Drift     float64 `json:"drift"`
	AbsDrift  float64 `json:"abs_drift"`
	VideoPTS  float64 `json:"video_pts"`
	AudioPTS  float64 `json:"audio_pts"`
}

// SyncAnalysis represents synchronization analysis results
type SyncAnalysis struct {
	TotalSyncIssues int         `json:"total_sync_issues"`
	MaxDrift        float64     `json:"max_drift"`
	AvgDrift        float64     `json:"avg_drift"`
	SyncIssues      []SyncIssue `json:"sync_issues"`
	AudioPackets    int         `json:"audio_packets"`
	VideoPackets    int         `json:"video_packets"`
	Error           string      `json:"error,omitempty"`
}

// NewPTSAnalyzer creates a new PTSAnalyzer instance
func NewPTSAnalyzer(videoFile string) *PTSAnalyzer {
	return &PTSAnalyzer{
		VideoFile: videoFile,
		PTSInfo:   make(map[string]interface{}),
	}
}

// RunCommand executes a command and returns stdout, stderr
func (a *PTSAnalyzer) RunCommand(cmd []string) (string, string, error) {
	command := exec.Command(cmd[0], cmd[1:]...)

	var stdout, stderr strings.Builder
	command.Stdout = &stdout
	command.Stderr = &stderr

	err := command.Run()
	return stdout.String(), stderr.String(), err
}

// GetStreamInfo gets stream information using ffprobe
func (a *PTSAnalyzer) GetStreamInfo() (*StreamInfo, error) {
	cmd := []string{
		"ffprobe", "-v", "quiet", "-print_format", "json",
		"-show_streams", "-show_format", a.VideoFile,
	}

	stdout, stderr, err := a.RunCommand(cmd)
	if err != nil {
		return nil, fmt.Errorf("ffprobe failed: %v, stderr: %s", err, stderr)
	}

	var streamInfo StreamInfo
	err = json.Unmarshal([]byte(stdout), &streamInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ffprobe output: %v", err)
	}

	return &streamInfo, nil
}

// ExtractPTSTimestamps extracts PTS values for audio and/or video streams
func (a *PTSAnalyzer) ExtractPTSTimestamps(streamType string) (map[string][]struct {
	PTS     float64
	PTSTime float64
}, error) {
	ptsData := map[string][]struct {
		PTS     float64
		PTSTime float64
	}{
		"audio": {},
		"video": {},
	}

	streamInfo, err := a.GetStreamInfo()
	if err != nil {
		return ptsData, err
	}

	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, stream := range streamInfo.Streams {
		if (stream.CodecType != "audio" && stream.CodecType != "video") ||
			(streamType != "both" && stream.CodecType != streamType) {
			continue
		}

		wg.Add(1)
		go func(stream Stream) {
			defer wg.Done()

			cmd := []string{
				"ffprobe", "-v", "quiet",
				"-select_streams", fmt.Sprintf("%d", stream.Index),
				"-show_entries", "packet=pts_time",
				"-of", "csv=p=0", a.VideoFile,
			}

			stdout, stderr, err := a.RunCommand(cmd)
			if err != nil {
				fmt.Printf("Warning: Could not extract PTS for %s stream: %v, stderr: %s\n",
					stream.CodecType, err, stderr)
				return
			}

			lines := strings.Split(strings.TrimSpace(stdout), "\n")
			timeBase := parseFraction(stream.TimeBase)

			mu.Lock()
			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line == "" || line == "N/A" {
					continue
				}

				// Handle any unexpected characters
				line = strings.TrimRight(line, ",")
				ptsTime, err := strconv.ParseFloat(line, 64)
				if err != nil {
					continue
				}

				pts := ptsTime / timeBase
				ptsData[stream.CodecType] = append(ptsData[stream.CodecType], struct {
					PTS     float64
					PTSTime float64
				}{PTS: pts, PTSTime: ptsTime})
			}
			mu.Unlock()

		}(stream)
	}

	wg.Wait()
	return ptsData, nil
}

// ExtractPTSTimestampsAlt alternative method to extract PTS values
func (a *PTSAnalyzer) ExtractPTSTimestampsAlt(streamType string) (map[string][]struct {
	PTS     float64
	PTSTime float64
}, error) {
	ptsData := map[string][]struct {
		PTS     float64
		PTSTime float64
	}{
		"audio": {},
		"video": {},
	}

	streamInfo, err := a.GetStreamInfo()
	if err != nil {
		return ptsData, err
	}

	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, stream := range streamInfo.Streams {
		if (stream.CodecType != "audio" && stream.CodecType != "video") ||
			(streamType != "both" && stream.CodecType != streamType) {
			continue
		}

		wg.Add(1)
		go func(stream Stream) {
			defer wg.Done()

			cmd := []string{
				"ffprobe", "-v", "quiet",
				"-select_streams", fmt.Sprintf("%d", stream.Index),
				"-show_frames",
				"-show_entries", "frame=pkt_pts_time",
				"-of", "csv=p=0", a.VideoFile,
			}

			stdout, stderr, err := a.RunCommand(cmd)
			if err != nil {
				fmt.Printf("Warning: Could not extract PTS for %s stream: %v, stderr: %s\n",
					stream.CodecType, err, stderr)
				return
			}

			lines := strings.Split(strings.TrimSpace(stdout), "\n")
			timeBase := parseFraction(stream.TimeBase)

			mu.Lock()
			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line == "" || line == "N/A" {
					continue
				}

				ptsTime, err := strconv.ParseFloat(line, 64)
				if err != nil {
					continue
				}

				pts := ptsTime / timeBase
				ptsData[stream.CodecType] = append(ptsData[stream.CodecType], struct {
					PTS     float64
					PTSTime float64
				}{PTS: pts, PTSTime: ptsTime})
			}
			mu.Unlock()

		}(stream)
	}

	wg.Wait()
	return ptsData, nil
}

// AnalyzePTSSync analyzes PTS synchronization between audio and video
func (a *PTSAnalyzer) AnalyzePTSSync(tolerance float64) *SyncAnalysis {
	result := &SyncAnalysis{}

	// Try the primary method first
	ptsData, err := a.ExtractPTSTimestamps("both")
	if err != nil {
		result.Error = fmt.Sprintf("Failed to extract PTS data: %v", err)
		return result
	}

	// If no data, try alternative method
	if len(ptsData["audio"]) == 0 || len(ptsData["video"]) == 0 {
		ptsData, err = a.ExtractPTSTimestampsAlt("both")
		if err != nil {
			result.Error = fmt.Sprintf("Failed to extract PTS data: %v", err)
			return result
		}
	}

	audioPTS := ptsData["audio"]
	videoPTS := ptsData["video"]

	if len(audioPTS) == 0 || len(videoPTS) == 0 {
		result.Error = "Missing audio or video PTS data"
		return result
	}

	var syncIssues []SyncIssue
	maxDrift := 0.0
	totalDrift := 0.0
	count := 0

	// Compare timestamps at similar time points
	for _, video := range videoPTS {
		// Find closest audio PTS
		closestAudio := findClosestAudio(audioPTS, video.PTSTime)
		drift := video.PTSTime - closestAudio.PTSTime // Positive means audio is behind video

		absDrift := math.Abs(drift)
		totalDrift += drift
		count++

		if absDrift > maxDrift {
			maxDrift = absDrift
		}

		if absDrift > tolerance {
			syncIssues = append(syncIssues, SyncIssue{
				VideoTime: video.PTSTime,
				AudioTime: closestAudio.PTSTime,
				Drift:     drift,
				AbsDrift:  absDrift,
				VideoPTS:  video.PTS,
				AudioPTS:  closestAudio.PTS,
			})
		}
	}

	avgDrift := 0.0
	if count > 0 {
		avgDrift = totalDrift / float64(count)
	}

	// Limit to first 10 issues
	if len(syncIssues) > 10 {
		syncIssues = syncIssues[:10]
	}

	result.TotalSyncIssues = len(syncIssues)
	result.MaxDrift = maxDrift
	result.AvgDrift = avgDrift
	result.SyncIssues = syncIssues
	result.AudioPackets = len(audioPTS)
	result.VideoPackets = len(videoPTS)

	return result
}

// FixAudioDrift fixes audio drift by adjusting the audio PTS values
func (a *PTSAnalyzer) FixAudioDrift(outputFile string, driftSeconds *float64) error {
	// Analyze sync to get drift information
	syncInfo := a.AnalyzePTSSync(0.04)

	if syncInfo.Error != "" {
		return fmt.Errorf("sync analysis error: %s", syncInfo.Error)
	}

	// Use provided drift or calculate average drift
	drift := syncInfo.AvgDrift
	if driftSeconds != nil {
		drift = *driftSeconds
	}
	fmt.Printf("Detected average audio drift: %.6f seconds\n", drift)

	if math.Abs(drift) < 0.001 { // Less than 1ms drift
		fmt.Println("No significant drift detected. Copying original file.")
		return copyFile(a.VideoFile, outputFile)
	}

	var filterComplex string
	if drift > 0 {
		// Audio is behind video, need to delay audio
		fmt.Printf("Delaying audio by %.6f seconds\n", drift)
		delayMs := int(drift * 1000)
		filterComplex = fmt.Sprintf("adelay=%d|%d", delayMs, delayMs)
	} else {
		// Audio is ahead of video, need to trim beginning
		fmt.Printf("Advancing audio by %.6f seconds\n", math.Abs(drift))
		filterComplex = fmt.Sprintf("atrim=start=%.6f,asetpts=PTS-STARTPTS", math.Abs(drift))
	}

	// Build ffmpeg command
	cmd := []string{
		"ffmpeg", "-i", a.VideoFile,
		"-filter_complex", fmt.Sprintf("[0:a]%s[aout]", filterComplex),
		"-map", "0:v", "-map", "[aout]",
		"-c:v", "copy", // Copy video stream
		"-c:a", "aac", // Re-encode audio
		"-y", // Overwrite output file
		outputFile,
	}

	fmt.Println("Fixing audio drift...")
	_, stderr, err := a.RunCommand(cmd)
	if err != nil {
		return fmt.Errorf("ffmpeg failed: %v, stderr: %s", err, stderr)
	}

	fmt.Printf("Successfully created fixed file: %s\n", outputFile)
	return nil
}

// FixAudioDriftAlt alternative method to fix audio drift
func (a *PTSAnalyzer) FixAudioDriftAlt(outputFile string, driftSeconds *float64) error {
	// Analyze sync to get drift information
	syncInfo := a.AnalyzePTSSync(0.04)

	if syncInfo.Error != "" {
		return fmt.Errorf("sync analysis error: %s", syncInfo.Error)
	}

	// Use provided drift or calculate average drift
	drift := syncInfo.AvgDrift
	if driftSeconds != nil {
		drift = *driftSeconds
	}
	fmt.Printf("Detected average audio drift: %.6f seconds\n", drift)

	if math.Abs(drift) < 0.001 { // Less than 1ms drift
		fmt.Println("No significant drift detected. Copying original file.")
		return copyFile(a.VideoFile, outputFile)
	}

	var filter string
	if drift > 0 {
		// Audio is behind video, need to add offset
		filter = fmt.Sprintf("asetpts=PTS+%.6f/TB", drift)
	} else {
		// Audio is ahead of video, need to subtract offset
		filter = fmt.Sprintf("asetpts=PTS-%.6f/TB", math.Abs(drift))
	}

	// Build ffmpeg command
	cmd := []string{
		"ffmpeg", "-i", a.VideoFile,
		"-af", filter,
		"-c:v", "copy", // Copy video stream
		"-c:a", "aac", // Re-encode audio
		"-y", // Overwrite output file
		outputFile,
	}

	fmt.Println("Fixing audio drift using asetpts filter...")
	_, stderr, err := a.RunCommand(cmd)
	if err != nil {
		return fmt.Errorf("ffmpeg failed: %v, stderr: %s", err, stderr)
	}

	fmt.Printf("Successfully created fixed file: %s\n", outputFile)
	return nil
}

// MonitorPTSRealtime monitors PTS values in real-time during playback
func (a *PTSAnalyzer) MonitorPTSRealtime() error {
	cmd := []string{
		"ffplay", "-i", a.VideoFile,
		"-an", "-vn", // Disable audio and video to just analyze
		"-stats",    // Show stats
		"-autoexit", // Exit when done
	}

	fmt.Printf("Starting real-time PTS monitoring at %s\n", time.Now().Format(time.RFC3339))
	a.RealTimeStart = time.Now()

	process := exec.Command(cmd[0], cmd[1:]...)
	stderr, err := process.StderrPipe()
	if err != nil {
		return err
	}

	if err := process.Start(); err != nil {
		return err
	}

	// Read stderr line by line
	scanner := bufio.NewScanner(stderr)
	ptsPattern := regexp.MustCompile(`pts:(\d+)\s+pts_time:([\d.]+)`)

	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "pts_time:") {
			matches := ptsPattern.FindStringSubmatch(line)
			if len(matches) == 3 {
				//pts, _ := strconv.ParseInt(matches[1], 10, 64)
				ptsTime, _ := strconv.ParseFloat(matches[2], 64)
				currentRealTime := time.Since(a.RealTimeStart).Seconds()

				drift := math.Abs(ptsTime - currentRealTime)

				fmt.Printf("PTS: %.3fs | Real: %.3fs | Drift: %.3fs\n", ptsTime, currentRealTime, drift)

				if drift > 0.1 { // Alert on significant drift
					fmt.Printf("⚠️  SYNC WARNING: %.3fs drift detected!\n", drift)
				}
			}
		}
	}

	return process.Wait()
}

// GeneratePTSReport generates a comprehensive PTS analysis report
func (a *PTSAnalyzer) GeneratePTSReport() string {
	var report strings.Builder

	// Get basic info
	streamInfo, err := a.GetStreamInfo()
	if err != nil {
		return fmt.Sprintf("Error getting stream info: %v", err)
	}

	syncAnalysis := a.AnalyzePTSSync(0.04)

	report.WriteString("=== PTS ANALYSIS REPORT ===\n\n")
	report.WriteString(fmt.Sprintf("File: %s\n", a.VideoFile))
	report.WriteString(fmt.Sprintf("Analysis Time: %s\n\n", time.Now().Format("2006-01-02 15:04:05")))

	// Stream information
	report.WriteString("STREAM INFORMATION:\n")
	for _, stream := range streamInfo.Streams {
		if stream.CodecType == "audio" || stream.CodecType == "video" {
			report.WriteString(fmt.Sprintf("  %s Stream %d:\n", strings.ToUpper(stream.CodecType), stream.Index))
			report.WriteString(fmt.Sprintf("    Codec: %s\n", stream.CodecName))
			report.WriteString(fmt.Sprintf("    Time Base: %s\n", stream.TimeBase))
			if stream.Duration != "" {
				report.WriteString(fmt.Sprintf("    Duration: %ss\n", stream.Duration))
			}
		}
	}

	// Synchronization analysis
	report.WriteString("\nSYNCHRONIZATION ANALYSIS:\n")
	if syncAnalysis.Error != "" {
		report.WriteString(fmt.Sprintf("  Error: %s\n", syncAnalysis.Error))
	} else {
		report.WriteString(fmt.Sprintf("  Audio Packets: %d\n", syncAnalysis.AudioPackets))
		report.WriteString(fmt.Sprintf("  Video Packets: %d\n", syncAnalysis.VideoPackets))
		report.WriteString(fmt.Sprintf("  Max Drift: %.4fs\n", syncAnalysis.MaxDrift))
		report.WriteString(fmt.Sprintf("  Avg Drift: %.4fs\n", syncAnalysis.AvgDrift))
		report.WriteString(fmt.Sprintf("  Sync Issues: %d\n", syncAnalysis.TotalSyncIssues))

		if len(syncAnalysis.SyncIssues) > 0 {
			report.WriteString("\n  First few sync issues:\n")
			for i, issue := range syncAnalysis.SyncIssues {
				if i >= 5 {
					break
				}
				driftDir := "audio behind"
				if issue.Drift < 0 {
					driftDir = "audio ahead"
				}
				report.WriteString(fmt.Sprintf("    Time %.3fs: %.4fs drift (%s)\n",
					issue.VideoTime, issue.AbsDrift, driftDir))
			}
		}
	}

	return report.String()
}

// Helper functions

func parseFraction(fraction string) float64 {
	parts := strings.Split(fraction, "/")
	if len(parts) != 2 {
		return 1.0
	}

	numerator, err1 := strconv.ParseFloat(parts[0], 64)
	denominator, err2 := strconv.ParseFloat(parts[1], 64)

	if err1 != nil || err2 != nil || denominator == 0 {
		return 1.0
	}

	return numerator / denominator
}

func findClosestAudio(audioPTS []struct {
	PTS     float64
	PTSTime float64
}, videoTime float64) struct {
	PTS     float64
	PTSTime float64
} {
	if len(audioPTS) == 0 {
		return struct {
			PTS     float64
			PTSTime float64
		}{}
	}

	closest := audioPTS[0]
	minDiff := math.Abs(audioPTS[0].PTSTime - videoTime)

	for _, audio := range audioPTS {
		diff := math.Abs(audio.PTSTime - videoTime)
		if diff < minDiff {
			minDiff = diff
			closest = audio
		}
	}

	return closest
}

func copyFile(src, dst string) error {
	input, err := os.ReadFile(src)
	if err != nil {
		return err
	}

	return os.WriteFile(dst, input, 0644)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: pts_analyzer <command> <video_file> [options]")
		fmt.Println("Commands:")
		fmt.Println("  analyze <file>              - Generate PTS analysis report")
		fmt.Println("  monitor <file>              - Monitor PTS in real-time")
		fmt.Println("  sync <file>                 - Check audio/video sync")
		fmt.Println("  fix <file> <output>         - Fix audio drift (auto-detect)")
		fmt.Println("  fix <file> <output> <drift> - Fix audio drift with specified value")
		fmt.Println("  fix2 <file> <output>        - Alternative fix method (auto-detect)")
		fmt.Println("  fix2 <file> <output> <drift>- Alternative fix with specified value")
		os.Exit(1)
	}

	command := os.Args[1]

	if command == "analyze" || command == "monitor" || command == "sync" {
		if len(os.Args) < 3 {
			fmt.Printf("Error: %s command requires a video file\n", command)
			os.Exit(1)
		}
		videoFile := os.Args[2]
		analyzer := NewPTSAnalyzer(videoFile)

		switch command {
		case "analyze":
			fmt.Println(analyzer.GeneratePTSReport())
		case "monitor":
			if err := analyzer.MonitorPTSRealtime(); err != nil {
				log.Fatalf("Monitoring error: %v", err)
			}
		case "sync":
			result := analyzer.AnalyzePTSSync(0.04)
			jsonData, err := json.MarshalIndent(result, "", "  ")
			if err != nil {
				log.Fatalf("JSON marshaling error: %v", err)
			}
			fmt.Println(string(jsonData))
		}
	} else if command == "fix" || command == "fix2" {
		if len(os.Args) < 4 {
			fmt.Printf("Error: %s command requires input and output files\n", command)
			os.Exit(1)
		}

		videoFile := os.Args[2]
		outputFile := os.Args[3]
		analyzer := NewPTSAnalyzer(videoFile)

		var driftPtr *float64
		if len(os.Args) > 4 {
			drift, err := strconv.ParseFloat(os.Args[4], 64)
			if err != nil {
				log.Fatalf("Invalid drift value: %v", err)
			}
			driftPtr = &drift
		}

		var err error
		if command == "fix" {
			err = analyzer.FixAudioDrift(outputFile, driftPtr)
		} else {
			err = analyzer.FixAudioDriftAlt(outputFile, driftPtr)
		}

		if err != nil {
			log.Fatalf("Error: %v", err)
		}
	} else {
		fmt.Println("Unknown command. Use 'analyze', 'monitor', 'sync', 'fix', or 'fix2'")
		os.Exit(1)
	}
}
