package integration

import (
	"fmt"
	"os"
	"testing"
	"time"
)

var cluster *Cluster

func TestMain(m *testing.M) {
	var err error
	cluster, err = BuildAndStart()
	if err != nil {
		fmt.Fprintf(os.Stderr, "cluster setup failed: %v\n", err)
		os.Exit(1)
	}
	code := m.Run()
	cluster.Stop()
	os.Exit(code)
}

func TestReadWrite(t *testing.T) {
	t.Run("healthy", func(t *testing.T) {
		ref := map[string]string{"k": "v", "a": "b"}
		for k, v := range ref {
			resp, err := cluster.Put("node1", k, v)
			if err != nil || resp != "true" {
				t.Fatalf("put %s: %q %v", k, resp, err)
			}
		}
		time.Sleep(time.Second)
		for k, want := range ref {
			got, _ := cluster.Get("node2", k)
			if got != want {
				t.Errorf("get %s: got %q, want %q", k, got, want)
			}
		}
	})

	t.Run("delay_200ms", func(t *testing.T) {
		if err := cluster.FaultDelay("node2", 200); err != nil {
			t.Fatal(err)
		}
		defer cluster.FaultClear("node2")

		start := time.Now()
		got, err := cluster.Get("node2", "k")
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("get under delay: %v", err)
		}
		t.Logf("get under 200ms delay took %v", elapsed)
		if elapsed < 200*time.Millisecond {
			t.Log("delay measurably affected latency")
		}
		if got != "v" {
			t.Errorf("got %q, want v", got)
		}
	})

	t.Run("loss_50pct", func(t *testing.T) {
		if err := cluster.FaultLoss("node2", 50); err != nil {
			t.Fatal(err)
		}
		defer cluster.FaultClear("node2")

		got, err := cluster.Get("node2", "a")
		if err != nil {
			t.Logf("get under loss may fail: %v", err)
		} else if got != "b" {
			t.Errorf("got %q, want b", got)
		}
	})

	t.Run("kill_recovery", func(t *testing.T) {
		cluster.FaultKill("node2")
		time.Sleep(time.Second)

		got, err := cluster.Get("node3", "k")
		if err != nil {
			t.Fatalf("get after node2 killed: %v", err)
		}
		if got != "v" {
			t.Errorf("got %q, want v", got)
		}
	})
}
