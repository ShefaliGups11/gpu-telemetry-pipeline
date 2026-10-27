package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

func Get(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func Int(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

// List splits a comma-separated env value.
func List(key, def string) []string {
	raw := Get(key, def)
	var out []string
	for _, p := range strings.Split(raw, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func Duration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

// ReplicaIndex prefers STREAMER_INDEX, then the ordinal suffix of HOSTNAME (StatefulSet).
func ReplicaIndex() int {
	if v := os.Getenv("STREAMER_INDEX"); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil {
			return n
		}
	}
	host := os.Getenv("HOSTNAME")
	i := strings.LastIndex(host, "-")
	if i < 0 || i == len(host)-1 {
		return 0
	}
	n, err := strconv.Atoi(host[i+1:])
	if err != nil {
		return 0
	}
	return n
}
