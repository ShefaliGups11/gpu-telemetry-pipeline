package config

import (
	"testing"
	"time"
)

func TestGet(t *testing.T) {
	t.Setenv("CFG_X", "hello")
	if Get("CFG_X", "d") != "hello" {
		t.Fatal("env")
	}
	if Get("CFG_MISSING", "d") != "d" {
		t.Fatal("default")
	}
}

func TestInt(t *testing.T) {
	if Int("NOPE", 7) != 7 {
		t.Fatal("default")
	}
	t.Setenv("N", "3")
	if Int("N", 7) != 3 {
		t.Fatal("parse")
	}
	t.Setenv("N", "x")
	if Int("N", 7) != 7 {
		t.Fatal("bad")
	}
}

func TestDuration(t *testing.T) {
	if Duration("NOPE", time.Second) != time.Second {
		t.Fatal("default")
	}
	t.Setenv("D", "250ms")
	if Duration("D", time.Second) != 250*time.Millisecond {
		t.Fatal("parse")
	}
	t.Setenv("D", "nope")
	if Duration("D", time.Second) != time.Second {
		t.Fatal("bad")
	}
}

func TestList(t *testing.T) {
	if len(List("NOPE", "")) != 0 {
		t.Fatal("empty")
	}
	t.Setenv("PEERS", " a:1, b:2 ")
	got := List("PEERS", "")
	if len(got) != 2 || got[0] != "a:1" || got[1] != "b:2" {
		t.Fatal(got)
	}
}

func TestReplicaIndex(t *testing.T) {
	t.Setenv("STREAMER_INDEX", "2")
	if ReplicaIndex() != 2 {
		t.Fatal("index")
	}
	t.Setenv("STREAMER_INDEX", "x")
	t.Setenv("HOSTNAME", "gpu-telemetry-streamer-4")
	if ReplicaIndex() != 4 {
		t.Fatal("ordinal")
	}
	t.Setenv("HOSTNAME", "noshard")
	if ReplicaIndex() != 0 {
		t.Fatal("no dash")
	}
	t.Setenv("HOSTNAME", "pod-")
	if ReplicaIndex() != 0 {
		t.Fatal("trailing")
	}
	t.Setenv("HOSTNAME", "pod-x")
	if ReplicaIndex() != 0 {
		t.Fatal("non int")
	}
}
