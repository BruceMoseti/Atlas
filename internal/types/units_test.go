package types

import "testing"

func TestParseBytes(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"1024", 1024},
		{"512B", 512},
		// Both forms mean 1024: Atlas is allocating memory, and nobody asking
		// for 512MB of RAM means 512,000,000 bytes.
		{"1KB", 1 << 10},
		{"1KiB", 1 << 10},
		{"512MB", 512 << 20},
		{"8GB", 8 << 30},
		{"1.5GB", 1536 << 20},
		{"2T", 2 << 40},
		{" 4gb ", 4 << 30},
	}
	for _, c := range cases {
		got, err := ParseBytes(c.in)
		if err != nil {
			t.Errorf("ParseBytes(%q) failed: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("ParseBytes(%q) = %d, want %d", c.in, got, c.want)
		}
	}

	for _, bad := range []string{"", "   ", "lots", "-5GB", "0MB", "GB"} {
		if got, err := ParseBytes(bad); err == nil {
			t.Errorf("ParseBytes(%q) = %d, want an error", bad, got)
		}
	}
}

func TestParseCPUMillis(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"1", 1000},
		{"4", 4000},
		{"2.5", 2500},
		{"0.5", 500},
		{" 8 ", 8000},
	}
	for _, c := range cases {
		got, err := ParseCPUMillis(c.in)
		if err != nil {
			t.Errorf("ParseCPUMillis(%q) failed: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("ParseCPUMillis(%q) = %d, want %d", c.in, got, c.want)
		}
	}
	for _, bad := range []string{"", "two", "0", "-1"} {
		if got, err := ParseCPUMillis(bad); err == nil {
			t.Errorf("ParseCPUMillis(%q) = %d, want an error", bad, got)
		}
	}
}

func TestResourceArithmetic(t *testing.T) {
	a := Resources{CPUMillis: 1000, MemoryBytes: 1 << 30}
	b := Resources{CPUMillis: 500, MemoryBytes: 512 << 20}

	if got := a.Add(b); got.CPUMillis != 1500 || got.MemoryBytes != 1536<<20 {
		t.Errorf("Add = %+v", got)
	}
	if got := a.Sub(b); got.CPUMillis != 500 || got.MemoryBytes != 512<<20 {
		t.Errorf("Sub = %+v", got)
	}
	// Sub is allowed to go negative; the store is what refuses to persist it.
	if got := b.Sub(a); got.CPUMillis != -500 {
		t.Errorf("Sub should be allowed to go negative, got %+v", got)
	}
}

func TestFitsRequiresBothDimensions(t *testing.T) {
	capacity := Resources{CPUMillis: 4000, MemoryBytes: 8 << 30}
	cases := []struct {
		name string
		req  Resources
		want bool
	}{
		{"exact fit", Resources{CPUMillis: 4000, MemoryBytes: 8 << 30}, true},
		{"cpu over", Resources{CPUMillis: 4001, MemoryBytes: 1 << 30}, false},
		{"memory over", Resources{CPUMillis: 100, MemoryBytes: 9 << 30}, false},
		{"both under", Resources{CPUMillis: 100, MemoryBytes: 1 << 20}, true},
	}
	for _, c := range cases {
		if got := c.req.Fits(capacity); got != c.want {
			t.Errorf("%s: Fits = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestFormatBytesRoundTripsThroughParse(t *testing.T) {
	for _, n := range []int64{512, 1 << 10, 1 << 20, 4 << 30, 2 << 40} {
		s := FormatBytes(n)
		back, err := ParseBytes(s)
		if err != nil {
			t.Errorf("FormatBytes(%d) produced %q, which ParseBytes rejects: %v", n, s, err)
			continue
		}
		if back != n {
			t.Errorf("%d formatted as %q parsed back as %d", n, s, back)
		}
	}
}
