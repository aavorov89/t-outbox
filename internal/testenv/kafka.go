//go:build integration

package testenv

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Kafka is a running Redpanda container.
type Kafka struct {
	ctr     *redpanda.Container
	Brokers []string
	admin   *kadm.Client
	adminCl *kgo.Client
}

// StartKafka starts Redpanda. Topics are not auto-created: use NewTopic.
func StartKafka(ctx context.Context) (*Kafka, error) {
	ctr, err := redpanda.Run(ctx, image("REDPANDA_IMAGE", "redpandadata/redpanda:v26.2.6"))
	if err != nil {
		return nil, fmt.Errorf("testenv: start redpanda: %w", err)
	}
	k := &Kafka{ctr: ctr}
	broker, err := ctr.KafkaSeedBroker(ctx)
	if err != nil {
		_ = k.Terminate(context.Background())
		return nil, fmt.Errorf("testenv: seed broker: %w", err)
	}
	k.Brokers = []string{broker}
	if k.adminCl, err = kgo.NewClient(kgo.SeedBrokers(broker)); err != nil {
		_ = k.Terminate(context.Background())
		return nil, fmt.Errorf("testenv: admin client: %w", err)
	}
	k.admin = kadm.NewClient(k.adminCl)
	return k, nil
}

// Terminate stops the container.
func (k *Kafka) Terminate(ctx context.Context) error {
	if k.adminCl != nil {
		k.adminCl.Close()
	}
	return testcontainers.TerminateContainer(k.ctr)
}

var topicUnsafe = regexp.MustCompile(`[^a-zA-Z0-9._-]+`)

// NewTopic creates a uniquely named topic for t with the given partition count and
// deletes it when t finishes.
func (k *Kafka) NewTopic(t testing.TB, partitions int32) string {
	t.Helper()
	name := strings.ToLower(topicUnsafe.ReplaceAllString(t.Name(), "_"))
	if len(name) > 180 {
		name = name[:180]
	}
	name += "." + randHex(4)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resp, err := k.admin.CreateTopic(ctx, partitions, 1, nil, name)
	if err == nil {
		err = resp.Err
	}
	if err != nil {
		t.Fatalf("testenv: create topic %s: %v", name, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = k.admin.DeleteTopics(ctx, name)
	})
	return name
}

// NewClient returns a client seeded with the brokers, closed when t finishes.
func (k *Kafka) NewClient(t testing.TB, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(k.Brokers...)}, opts...)...)
	if err != nil {
		t.Fatalf("testenv: kafka client: %v", err)
	}
	t.Cleanup(cl.Close)
	return cl
}

// ConsumeN reads topic from the start until n records arrive or timeout passes. It fails
// t if fewer than n arrive. It does not check that no more than n exist; to assert an
// exact count, call it with n+1 and a short timeout via TryConsumeN.
func (k *Kafka) ConsumeN(t testing.TB, topic string, n int, timeout time.Duration) []*kgo.Record {
	t.Helper()
	recs := k.TryConsumeN(t, topic, n, timeout)
	if len(recs) < n {
		t.Fatalf("testenv: consumed %d of %d records from %s within %v", len(recs), n, topic, timeout)
	}
	return recs
}

// TryConsumeN is ConsumeN without failing: it returns whatever arrived in time.
func (k *Kafka) TryConsumeN(t testing.TB, topic string, n int, timeout time.Duration) []*kgo.Record {
	t.Helper()
	cl := k.NewClient(t,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var out []*kgo.Record
	for len(out) < n {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			break
		}
		fetches.EachError(func(topic string, p int32, err error) {
			t.Logf("testenv: fetch %s/%d: %v", topic, p, err)
		})
		out = append(out, fetches.Records()...)
	}
	return out
}

// Pause freezes the broker (docker pause). Ports stay mapped and open connections hang,
// like a network partition. Unlike Stop/Start, the host port does not change, so
// clients reconnect after Unpause. The container is unpaused on cleanup if still paused.
func (k *Kafka) Pause(t testing.TB) {
	t.Helper()
	cli := dockerClient(t)
	ctx := context.Background()
	if _, err := cli.ContainerPause(ctx, k.ctr.GetContainerID(), client.ContainerPauseOptions{}); err != nil {
		t.Fatalf("testenv: pause kafka: %v", err)
	}
	t.Cleanup(func() {
		// Ignore "not paused" errors: the test may have unpaused already.
		_, _ = cli.ContainerUnpause(context.Background(), k.ctr.GetContainerID(), client.ContainerUnpauseOptions{})
	})
}

// Unpause resumes a paused broker.
func (k *Kafka) Unpause(t testing.TB) {
	t.Helper()
	if _, err := dockerClient(t).ContainerUnpause(context.Background(), k.ctr.GetContainerID(), client.ContainerUnpauseOptions{}); err != nil {
		t.Fatalf("testenv: unpause kafka: %v", err)
	}
}

func dockerClient(t testing.TB) *testcontainers.DockerClient {
	t.Helper()
	cli, err := testcontainers.NewDockerClientWithOpts(context.Background())
	if err != nil {
		t.Fatalf("testenv: docker client: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}
