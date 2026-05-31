package integration

import (
	"bufio"
	"fmt"
	"net"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

type Cluster struct {
	composeFile string
	projectRoot string
	nodes       []string
}

func projectRootDir() string {
	_, b, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(b), "../..")
}

func BuildAndStart() (*Cluster, error) {
	root := projectRootDir()
	c := &Cluster{
		composeFile: filepath.Join(root, "deploy", "docker-compose.yml"),
		projectRoot: root,
		nodes:       []string{"node1", "node2", "node3"},
	}

	if err := c.build(); err != nil {
		return nil, fmt.Errorf("build image: %w", err)
	}
	if err := c.up(); err != nil {
		return nil, fmt.Errorf("compose up: %w", err)
	}
	time.Sleep(8 * time.Second)
	return c, nil
}

func (c *Cluster) Stop() error {
	cmd := exec.Command("docker", "compose", "-p", "dht",
		"-f", c.composeFile, "down")
	return cmd.Run()
}

func (c *Cluster) build() error {
	cmd := exec.Command("docker", "build", "--no-cache",
		"-t", "dht-cluster",
		"-f", filepath.Join(c.projectRoot, "deploy", "Dockerfile"),
		".")
	cmd.Dir = c.projectRoot
	return cmd.Run()
}

func (c *Cluster) up() error {
	cmd := exec.Command("docker", "compose", "-p", "dht",
		"-f", c.composeFile, "up", "-d")
	return cmd.Run()
}

func (c *Cluster) ContainerName(node string) string {
	return fmt.Sprintf("dht-%s-1", node)
}

func (c *Cluster) CmdPort(node string) int {
	switch node {
	case "node1":
		return 21001
	case "node2":
		return 21002
	case "node3":
		return 21003
	default:
		return 21001
	}
}

func (c *Cluster) Put(node, key, value string) (string, error) {
	return c.cmd(node, fmt.Sprintf("put %s %s", key, value))
}

func (c *Cluster) Get(node, key string) (string, error) {
	return c.cmd(node, fmt.Sprintf("get %s", key))
}

func (c *Cluster) Delete(node, key string) (string, error) {
	return c.cmd(node, fmt.Sprintf("delete %s", key))
}

func (c *Cluster) cmd(node, line string) (string, error) {
	port := c.CmdPort(node)
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 3*time.Second)
	if err != nil {
		return "", fmt.Errorf("connect cmd port %d: %w", port, err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(30 * time.Second))
	fmt.Fprintf(conn, "%s\n", line)
	resp, _ := bufio.NewReader(conn).ReadString('\n')
	return strings.TrimSpace(resp), nil
}

func (c *Cluster) Exec(node string, args ...string) (string, error) {
	name := c.ContainerName(node)
	cmd := exec.Command("docker", append([]string{"exec", "-i", name}, args...)...)
	out, err := cmd.Output()
	return strings.TrimSpace(string(out)), err
}

func (c *Cluster) FaultDelay(node string, ms int) error {
	_, err := c.Exec(node, "tc", "qdisc", "add", "dev", "eth0", "root", "netem",
		"delay", fmt.Sprintf("%dms", ms))
	return err
}

func (c *Cluster) FaultDelayReplace(node string, ms int) error {
	_, err := c.Exec(node, "tc", "qdisc", "replace", "dev", "eth0", "root", "netem",
		"delay", fmt.Sprintf("%dms", ms))
	return err
}

func (c *Cluster) FaultLoss(node string, pct int) error {
	_, err := c.Exec(node, "tc", "qdisc", "replace", "dev", "eth0", "root", "netem",
		"loss", fmt.Sprintf("%d%%", pct))
	return err
}

func (c *Cluster) FaultClear(node string) error {
	_, err := c.Exec(node, "tc", "qdisc", "del", "dev", "eth0", "root")
	return err
}

func (c *Cluster) FaultKill(node string) error {
	name := c.ContainerName(node)
	cmd := exec.Command("docker", "kill", name)
	return cmd.Run()
}

func (c *Cluster) FaultPause(node string) error {
	name := c.ContainerName(node)
	cmd := exec.Command("docker", "pause", name)
	return cmd.Run()
}

func (c *Cluster) FaultUnpause(node string) error {
	name := c.ContainerName(node)
	cmd := exec.Command("docker", "unpause", name)
	return cmd.Run()
}

func (c *Cluster) FaultPartition(node string) error {
	_, err := c.Exec(node, "sh", "-c",
		"ip link set eth0 down")
	return err
}

func (c *Cluster) FaultReconnect(node string) error {
	_, err := c.Exec(node, "sh", "-c",
		"ip link set eth0 up")
	return err
}

func (c *Cluster) RunningNodes() []string {
	var running []string
	for _, node := range c.nodes {
		name := c.ContainerName(node)
		cmd := exec.Command("docker", "inspect",
			"--format", "{{.State.Status}}", name)
		out, err := cmd.Output()
		if err != nil {
			continue
		}
		if strings.TrimSpace(string(out)) == "running" {
			running = append(running, node)
		}
	}
	return running
}

func (c *Cluster) ContainerLogs(node string, lines int) (string, error) {
	name := c.ContainerName(node)
	cmd := exec.Command("docker", "logs", "--tail",
		fmt.Sprintf("%d", lines), name)
	out, err := cmd.Output()
	return string(out), err
}

type Op struct {
	Type  string
	Key   string
	Value string
}

func GenerateWorkload(n int) []Op {
	ops := make([]Op, n)
	for i := 0; i < n; i++ {
		switch i % 3 {
		case 0:
			ops[i] = Op{"put", fmt.Sprintf("key_%d", i), fmt.Sprintf("val_%d", i)}
		case 1:
			ops[i] = Op{"get", fmt.Sprintf("key_%d", i-1), ""}
		case 2:
			ops[i] = Op{"delete", fmt.Sprintf("key_%d", i-2), ""}
		}
	}
	return ops
}
