// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
// Adapted from Apache Kvrocks (Apache 2.0 License).

package util

import (
	"bufio"
	"fmt"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

// TCPClient is a raw RESP protocol client for testing protocol-level behavior.
type TCPClient struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
}

func NewTCPClient(conn net.Conn) *TCPClient {
	return &TCPClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
	}
}

func (c *TCPClient) Close() error {
	return c.conn.Close()
}

// WriteArgs sends a RESP array command.
func (c *TCPClient) WriteArgs(args ...string) error {
	s := fmt.Sprintf("*%d\r\n", len(args))
	for _, arg := range args {
		s += fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
	}
	_, err := c.writer.WriteString(s)
	if err != nil {
		return err
	}
	return c.writer.Flush()
}

// Write sends raw bytes.
func (c *TCPClient) Write(s string) error {
	_, err := c.writer.WriteString(s)
	if err != nil {
		return err
	}
	return c.writer.Flush()
}

// ReadLine reads one CRLF-terminated line (without the CRLF).
func (c *TCPClient) ReadLine() (string, error) {
	line, err := c.reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) >= 2 && line[len(line)-2] == '\r' {
		line = line[:len(line)-2]
	} else if len(line) >= 1 && line[len(line)-1] == '\n' {
		line = line[:len(line)-1]
	}
	return line, nil
}

// MustRead asserts the next line equals the expected string.
func (c *TCPClient) MustRead(t testing.TB, expected string) {
	line, err := c.ReadLine()
	require.NoError(t, err)
	require.Equal(t, expected, line)
}

// MustReadBulkString reads a RESP bulk string and asserts its value.
func (c *TCPClient) MustReadBulkString(t testing.TB, expected string) {
	line, err := c.ReadLine()
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("$%d", len(expected)), line)
	line, err = c.ReadLine()
	require.NoError(t, err)
	require.Equal(t, expected, line)
}
