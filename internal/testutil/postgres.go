package testutil

import (
	"context"
	"errors"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var errDockerMissing = errors.New("docker executable not found")

var (
	pgOnce sync.Once
	pgURL  string
	pgErr  error
)

var findDocker = dockerBin
var command = exec.Command
var pingFn = ping
var pgWait = 45 * time.Second
var pgPingEvery = 400 * time.Millisecond

func dockerBin() string {
	p, err := exec.LookPath("docker")
	if err != nil {
		return ""
	}
	return p
}

// PostgresURL starts (or reuses) a disposable Postgres container on localhost:15432.
func PostgresURL(t *testing.T) string {
	t.Helper()
	pgOnce.Do(func() {
		pgURL = "postgres://gpu:gpu@127.0.0.1:15432/gpu?sslmode=disable"
		if pingFn(pgURL) {
			return
		}
		docker := findDocker()
		if docker == "" {
			pgErr = errDockerMissing
			return
		}
		_ = command(docker, "rm", "-f", "gpu-pg-cov").Run()
		out, err := command(docker, "run", "-d", "--rm", "--name", "gpu-pg-cov",
			"-e", "POSTGRES_PASSWORD=gpu", "-e", "POSTGRES_USER=gpu", "-e", "POSTGRES_DB=gpu",
			"-p", "15432:5432", "postgres:16-alpine").CombinedOutput()
		if err != nil {
			pgErr = errors.New(string(out) + err.Error())
			return
		}
		deadline := time.Now().Add(pgWait)
		for time.Now().Before(deadline) {
			if pingFn(pgURL) {
				return
			}
			time.Sleep(pgPingEvery)
		}
		pgErr = context.DeadlineExceeded
	})
	if pgErr != nil || pgURL == "" {
		t.Skip("postgres test container:", pgErr)
	}
	return pgURL
}

func ping(url string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return false
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return false
	}
	defer pool.Close()
	return pool.Ping(ctx) == nil
}
