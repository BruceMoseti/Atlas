package simulator

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"
)

// Environment describes the machine a benchmark ran on.
//
// Every benchmark report starts with this block. A throughput number without the
// hardware it was measured on is not a result, it is a rumour, and the first thing
// anyone reading docs/RESULTS.md should be able to do is reproduce it or explain why
// their number differs.
func Environment() string {
	var b strings.Builder
	host, _ := os.Hostname()
	fmt.Fprintf(&b, "Atlas simulator\n")
	fmt.Fprintf(&b, "  run at       %s\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&b, "  host         %s\n", host)
	fmt.Fprintf(&b, "  go           %s\n", runtime.Version())
	fmt.Fprintf(&b, "  os/arch      %s/%s\n", runtime.GOOS, runtime.GOARCH)
	fmt.Fprintf(&b, "  cpus         %d\n", runtime.NumCPU())
	if mem := totalMemoryBytes(); mem > 0 {
		fmt.Fprintf(&b, "  memory       %s\n", formatBytes(mem))
	}
	return b.String()
}

// totalMemoryBytes reads MemTotal from /proc/meminfo, returning 0 where that is not
// available.
func totalMemoryBytes() int64 {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "MemTotal:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0
		}
		var kb int64
		if _, err := fmt.Sscanf(fields[1], "%d", &kb); err != nil {
			return 0
		}
		return kb * 1024
	}
	return 0
}
