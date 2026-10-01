package types

import (
	"fmt"
	"strconv"
	"strings"
)

// ParseCPUMillis accepts a number of cores and returns millicores.
func ParseCPUMillis(s string) (int64, error) {
	cores, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return 0, fmt.Errorf("invalid cpu %q: want a number of cores like 4 or 2.5", s)
	}
	if cores <= 0 {
		return 0, fmt.Errorf("invalid cpu %q: must be positive", s)
	}
	return int64(cores * 1000), nil
}

// ParseBytes accepts a plain byte count or a suffixed size.
//
// Both "1KB" and "1KiB" mean 1024 bytes. Atlas is allocating memory, and nobody
// asking for 512MB of RAM means 512,000,000 bytes.
func ParseBytes(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty size")
	}
	upper := strings.ToUpper(s)

	suffixes := []struct {
		suffix string
		mult   int64
	}{
		{"KIB", 1 << 10}, {"MIB", 1 << 20}, {"GIB", 1 << 30}, {"TIB", 1 << 40},
		{"KB", 1 << 10}, {"MB", 1 << 20}, {"GB", 1 << 30}, {"TB", 1 << 40},
		{"K", 1 << 10}, {"M", 1 << 20}, {"G", 1 << 30}, {"T", 1 << 40},
		{"B", 1},
	}
	for _, m := range suffixes {
		if strings.HasSuffix(upper, m.suffix) {
			num := strings.TrimSpace(upper[:len(upper)-len(m.suffix)])
			v, err := strconv.ParseFloat(num, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid size %q", s)
			}
			if v <= 0 {
				return 0, fmt.Errorf("invalid size %q: must be positive", s)
			}
			return int64(v * float64(m.mult)), nil
		}
	}
	v, err := strconv.ParseInt(upper, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q: want a byte count or a suffixed value like 512MB", s)
	}
	return v, nil
}

// FormatBytes renders a byte count with a binary suffix, for human-facing output.
func FormatBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit && exp < 4; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.3g%ciB", float64(n)/float64(div), "KMGTP"[exp])
}

// FormatCPU renders millicores as cores.
func FormatCPU(millis int64) string {
	return strconv.FormatFloat(float64(millis)/1000.0, 'g', -1, 64)
}
