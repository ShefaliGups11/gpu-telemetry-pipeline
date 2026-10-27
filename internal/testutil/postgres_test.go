package testutil

import (
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func resetPG() {
	pgOnce = sync.Once{}
	pgURL = ""
	pgErr = nil
	findDocker = dockerBin
	command = exec.Command
	pingFn = ping
	pgWait = 45 * time.Second
	pgPingEvery = 400 * time.Millisecond
}

func TestPingBadURL(t *testing.T) {
	if ping("postgres://gpu:gpu@127.0.0.1:1/gpu?sslmode=disable") {
		t.Fatal("expected ping fail")
	}
	if ping("://nope") {
		t.Fatal("parse")
	}
}

func TestDockerBinLookPath(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"docker", "docker.exe"} {
		exe := filepath.Join(dir, name)
		if err := os.WriteFile(exe, []byte("fake"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", dir)
	got := dockerBin()
	if got == "" {
		t.Fatal("expected LookPath hit")
	}
}

func TestDockerBinMissing(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	if got := dockerBin(); got != "" {
		t.Fatalf("expected empty, got %q", got)
	}
}

func TestPingLiveIfPresent(t *testing.T) {
	if !ping("postgres://gpu:gpu@127.0.0.1:15432/gpu?sslmode=disable") {
		t.Skip("no postgres on 15432")
	}
}

func TestPostgresURLAlreadyUp(t *testing.T) {
	resetPG()
	t.Cleanup(resetPG)
	pingFn = func(string) bool { return true }
	u := PostgresURL(t)
	if u == "" {
		t.Fatal("url")
	}
}

func TestPostgresURLSkipForced(t *testing.T) {
	resetPG()
	t.Cleanup(resetPG)
	pingFn = func(string) bool { return false }
	findDocker = func() string { return "" }
	t.Run("skip", func(st *testing.T) {
		PostgresURL(st)
		st.Fatal("should skip")
	})
}

func TestPostgresURLDockerRunFail(t *testing.T) {
	resetPG()
	t.Cleanup(resetPG)
	pingFn = func(string) bool { return false }
	findDocker = func() string { return "docker" }
	command = func(name string, args ...string) *exec.Cmd {
		return exec.Command("go", "__no_such_go_command__")
	}
	t.Run("skip", func(st *testing.T) {
		PostgresURL(st)
		st.Fatal("should skip")
	})
}

func TestPostgresURLWaitThenReady(t *testing.T) {
	resetPG()
	t.Cleanup(resetPG)
	n := 0
	pingFn = func(string) bool {
		n++
		return n > 1
	}
	findDocker = func() string { return "docker" }
	command = func(name string, args ...string) *exec.Cmd {
		return exec.Command("go", "version")
	}
	pgWait = time.Second
	pgPingEvery = time.Millisecond
	if PostgresURL(t) == "" {
		t.Fatal("url")
	}
}

func TestPostgresURLWaitTimeout(t *testing.T) {
	resetPG()
	t.Cleanup(resetPG)
	pingFn = func(string) bool { return false }
	findDocker = func() string { return "docker" }
	command = func(name string, args ...string) *exec.Cmd {
		return exec.Command("go", "version")
	}
	pgWait = 5 * time.Millisecond
	pgPingEvery = time.Millisecond
	t.Run("skip", func(st *testing.T) {
		PostgresURL(st)
		st.Fatal("should skip")
	})
}
