// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Adapted from Apache Kvrocks (Apache 2.0 License).

package util

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

const defaultGracePeriod = 30 * time.Second

// NeoKVServer wraps a neo_redis_standalone process for integration testing.
type NeoKVServer struct {
	t   testing.TB
	cmd *exec.Cmd

	addr *net.TCPAddr
	dir  string

	clean func(bool)
}

func (s *NeoKVServer) HostPort() string {
	return s.addr.AddrPort().String()
}

func (s *NeoKVServer) Host() string {
	return s.addr.AddrPort().Addr().String()
}

func (s *NeoKVServer) Port() uint64 {
	return uint64(s.addr.AddrPort().Port())
}

// NewClient creates a go-redis client connected to this server.
func (s *NeoKVServer) NewClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         s.addr.String(),
		DialTimeout:  30 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	})
}

// NewTCPClient creates a raw TCP client for protocol-level testing.
func (s *NeoKVServer) NewTCPClient() *TCPClient {
	c, err := net.Dial(s.addr.Network(), s.addr.String())
	require.NoError(s.t, err)
	return NewTCPClient(c)
}

// Close sends SIGTERM and waits for the process to exit.
func (s *NeoKVServer) Close() {
	s.close(false)
}

func (s *NeoKVServer) close(keepDir bool) {
	_ = s.cmd.Process.Signal(syscall.SIGTERM)

	done := make(chan error, 1)
	go func() { done <- s.cmd.Wait() }()

	select {
	case <-done:
		// Process exited (possibly with non-zero exit code, which is OK for test cleanup).
	case <-time.After(defaultGracePeriod):
		_ = s.cmd.Process.Kill()
		<-done
	}
	s.clean(keepDir)
}

// StartServer starts a neo_redis_standalone instance and waits until it is ready.
func StartServer(t testing.TB, configs map[string]string) *NeoKVServer {
	b := *binPath
	require.NotEmpty(t, b, "please set the binary path by -binPath")

	// Find a free port for Redis.
	redisAddr, err := findFreePort()
	require.NoError(t, err)

	// Create a temp directory for this test's data.
	dir := *workspace
	require.NotEmpty(t, dir, "please set the workspace by -workspace")
	dir, err = os.MkdirTemp(dir, fmt.Sprintf("%s-%d-*", t.Name(), time.Now().UnixMilli()))
	require.NoError(t, err)

	// Build command line arguments.
	args := []string{
		fmt.Sprintf("--redis_port=%d", redisAddr.Port),
		fmt.Sprintf("--data_dir=%s", filepath.Join(dir, "data")),
		"--leader_timeout_ms=15000",
	}
	// Apply any extra config overrides.
	for k, v := range configs {
		args = append(args, fmt.Sprintf("--%s=%s", k, v))
	}

	cmd := exec.Command(b, args...)

	stdout, err := os.Create(filepath.Join(dir, "stdout"))
	require.NoError(t, err)
	stderr, err := os.Create(filepath.Join(dir, "stderr"))
	require.NoError(t, err)

	// We need to read stdout to detect the NEOKV_READY message.
	stdoutPipe, err := cmd.StdoutPipe()
	require.NoError(t, err)
	cmd.Stderr = stderr

	require.NoError(t, cmd.Start())

	// Wait for the NEOKV_READY message or timeout.
	readyCh := make(chan bool, 1)
	go func() {
		scanner := bufio.NewScanner(stdoutPipe)
		for scanner.Scan() {
			line := scanner.Text()
			// Also write to stdout file for debugging.
			_, _ = fmt.Fprintln(stdout, line)
			if strings.HasPrefix(line, "NEOKV_READY") {
				readyCh <- true
				break
			}
		}
		// Continue draining stdout to the file in background.
		go func() {
			for scanner.Scan() {
				_, _ = fmt.Fprintln(stdout, scanner.Text())
			}
		}()
	}()

	select {
	case <-readyCh:
		// Server is ready.
	case <-time.After(60 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatalf("neo_redis_standalone did not become ready within 60 seconds")
	}

	// Double-check with PING.
	c := redis.NewClient(&redis.Options{Addr: redisAddr.String()})
	defer func() { require.NoError(t, c.Close()) }()
	require.Eventually(t, func() bool {
		return c.Ping(context.Background()).Err() == nil
	}, 10*time.Second, 200*time.Millisecond)

	return &NeoKVServer{
		t:    t,
		cmd:  cmd,
		addr: redisAddr,
		dir:  dir,
		clean: func(keepDir bool) {
			_ = stdout.Close()
			_ = stderr.Close()
			if *deleteOnExit && !keepDir {
				_ = os.RemoveAll(dir)
			}
		},
	}
}

func findFreePort() (*net.TCPAddr, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return nil, err
	}
	lis, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return nil, err
	}
	defer func() { _ = lis.Close() }()
	return lis.Addr().(*net.TCPAddr), nil
}
