package main

import (
	"reflect"
	"testing"
)

// flagsFirst exists because Go's flag package stops parsing at the first
// positional argument, which would make `atlas cancel JOB --reason x` silently
// drop the reason. The reordering is subtle enough to be worth pinning down:
// getting the value-flag case wrong would swallow the job id.
func TestFlagsFirst(t *testing.T) {
	cases := []struct {
		name string
		in   []string
		want []string
	}{
		{
			name: "already in flag-first order is unchanged",
			in:   []string{"--reason", "because", "job_1"},
			want: []string{"--reason", "because", "job_1"},
		},
		{
			name: "trailing value flag is moved with its value",
			in:   []string{"job_1", "--reason", "because"},
			want: []string{"--reason", "because", "job_1"},
		},
		{
			name: "trailing boolean flag does not swallow the positional",
			in:   []string{"w2", "--undo"},
			want: []string{"--undo", "w2"},
		},
		{
			name: "boolean flag before a positional leaves the positional alone",
			in:   []string{"--undo", "w2"},
			want: []string{"--undo", "w2"},
		},
		{
			name: "equals form needs no value lookahead",
			in:   []string{"job_1", "--reason=because"},
			want: []string{"--reason=because", "job_1"},
		},
		{
			name: "multiple flags and one positional",
			in:   []string{"--state", "QUEUED", "--limit", "5"},
			want: []string{"--state", "QUEUED", "--limit", "5"},
		},
		{
			name: "mixed ordering",
			in:   []string{"--limit", "5", "job_1", "--reason", "x"},
			want: []string{"--limit", "5", "--reason", "x", "job_1"},
		},
		{
			name: "double dash passes the remainder through as positional",
			in:   []string{"--reason", "x", "--", "--not-a-flag"},
			want: []string{"--reason", "x", "--not-a-flag"},
		},
		{
			name: "no arguments",
			in:   nil,
			want: nil,
		},
		{
			name: "only a positional",
			in:   []string{"job_1"},
			want: []string{"job_1"},
		},
		{
			name: "unknown trailing flag is not treated as taking a value",
			in:   []string{"job_1", "--watch"},
			want: []string{"--watch", "job_1"},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := flagsFirst(c.in)
			if len(got) == 0 && len(c.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("flagsFirst(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

// Every flag named in valueFlags must actually take a value on some subcommand,
// and no boolean flag may appear, or it would consume the positional after it.
func TestValueFlagsContainsNoBooleans(t *testing.T) {
	booleans := []string{"watch", "undo", "wait", "retry-on-process-exit", "retry-on-timeout"}
	for _, b := range booleans {
		if valueFlags[b] {
			t.Errorf("%q is a boolean flag but is listed in valueFlags; it would swallow the positional after it", b)
		}
	}
}
