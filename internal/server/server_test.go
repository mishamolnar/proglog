package server

import (
	"context"
	log_v1 "github.com/mishamolnar/proglog/api/v1"
	"github.com/mishamolnar/proglog/internal/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"net"
	"os"
	"sync"
	"testing"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, client log_v1.LogClient, config *Config){
		"produce/consume a message to/from log succeeds": testProduceConsume,
		"consume past log boundary fails":                testConsumePastLogBoundaryFails,
		"produce stream succeeds":                        testProduceStream,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTest(t)
			defer teardown()
			fn(t, client, config)
		})
	}
}

func setupTest(t *testing.T) (log_v1.LogClient, *Config, func()) { //creates server and returns log client!, and not server itself. Also config and teardown function
	t.Helper()
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	clientOptions := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	clientConn, err := grpc.Dial(listener.Addr().String(), clientOptions...)
	require.NoError(t, err)

	dir, err := os.MkdirTemp("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	cfg := &Config{CommitLog: clog}
	server, err := NewGRPCServer(cfg)
	require.NoError(t, err)

	go func() {
		server.Serve(listener)
	}()
	client := log_v1.NewLogClient(clientConn)
	return client, cfg, func() {
		server.Stop()
		clientConn.Close()
		listener.Close()
		clog.Remove()
	}
}

func testProduceConsume(t *testing.T, client log_v1.LogClient, config *Config) {
	ctx := context.Background()
	want := &log_v1.Record{Value: []byte("Hello world")}
	produce, err := client.Produce(ctx, &log_v1.ProduceRequest{Record: want})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &log_v1.ConsumeRequest{Offset: produce.Offset})
	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

func testConsumePastLogBoundaryFails(t *testing.T, client log_v1.LogClient, config *Config) {
	ctx := context.Background()
	rec := &log_v1.Record{Value: []byte("hello world")}
	produce, err := client.Produce(ctx, &log_v1.ProduceRequest{Record: rec})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &log_v1.ConsumeRequest{Offset: produce.Offset + 1})
	require.Nil(t, consume)
	got := status.Code(err)
	want := status.Code(log_v1.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	require.Equal(t, got, want)
}

func testProduceStream(t *testing.T, client log_v1.LogClient, config *Config) {
	ctx := context.Background()

	logProduceStream, err := client.ProduceStream(ctx)
	want := &log_v1.Record{Value: []byte("Hello world")}
	require.NoError(t, err)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = logProduceStream.Send(&log_v1.ProduceRequest{Record: want})
		require.NoError(t, err)
	}()
	wg.Wait()
	produceResp, err := logProduceStream.Recv()
	require.NoError(t, err)
	require.NotNil(t, produceResp)
}

func testProduceConsumeStream(t *testing.T, client log_v1.LogClient, config *Config) {
	ctx := context.Background()
	records := []*log_v1.Record{{Value: []byte("Hello world"), Offset: 0}, {Value: []byte("Hello world"), Offset: 1}}

	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)
		for offset, record := range records {
			err := stream.Send(&log_v1.ProduceRequest{Record: record})
			require.NoError(t, err)
			resp, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, offset, resp.Offset)
		}
	}
	{
		stream, err := client.ConsumeStream(ctx, &log_v1.ConsumeRequest{Offset: 0})
		require.NoError(t, err)
		for off, record := range records {
			resp, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, record.Value, resp.Record.Value)
			require.Equal(t, off, resp.Record.Offset)
		}
	}

}
