package grpc_clients

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/RohitIndira/Algo-Treading/pkg/correlation"
	common "github.com/RohitIndira/Algo-Treading/api/proto/common"
	pb "github.com/RohitIndira/Algo-Treading/api/proto/user_config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type UserConfigClient struct {
	client  pb.UserConfigServiceClient
	conn    *grpc.ClientConn
	timeout time.Duration
}

// correlationInterceptor forwards the correlation ID from the Go context into
// outgoing gRPC metadata so that downstream services can read it.
func correlationInterceptor(ctx context.Context, method string, req, reply interface{},
	cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if id := correlation.FromContext(ctx); id != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, correlation.GRPCMetadataKey, id)
	}
	return invoker(ctx, method, req, reply, cc, opts...)
}

func NewUserConfigClient(addr string, timeout time.Duration) (*UserConfigClient, error) {
	dialOpt, err := grpcDialOption()
	if err != nil {
		return nil, err
	}

	conn, err := grpc.Dial(addr, dialOpt, grpc.WithUnaryInterceptor(correlationInterceptor))
	if err != nil {
		return nil, err
	}

	return &UserConfigClient{
		client:  pb.NewUserConfigServiceClient(conn),
		conn:    conn,
		timeout: timeout,
	}, nil
}

// grpcDialOption returns a TLS credential when GRPC_TLS_CERT + GRPC_TLS_KEY are
// set, otherwise falls back to insecure (plain-text) for local/Docker deployments.
// Set GRPC_TLS_CA to a PEM file to pin a specific CA (e.g. self-signed).
func grpcDialOption() (grpc.DialOption, error) {
	certFile := os.Getenv("GRPC_TLS_CERT")
	keyFile := os.Getenv("GRPC_TLS_KEY")
	caFile := os.Getenv("GRPC_TLS_CA")

	if certFile == "" || keyFile == "" {
		log.Println("WARN gRPC client TLS disabled — set GRPC_TLS_CERT and GRPC_TLS_KEY to enable")
		return grpc.WithTransportCredentials(insecure.NewCredentials()), nil
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("gRPC TLS: load key pair: %w", err)
	}

	tlsCfg := &tls.Config{Certificates: []tls.Certificate{cert}}

	if caFile != "" {
		caPEM, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("gRPC TLS: read CA cert: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("gRPC TLS: failed to parse CA cert from %s", caFile)
		}
		tlsCfg.RootCAs = pool
	}

	log.Println("gRPC client TLS enabled")
	return grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)), nil
}

func (c *UserConfigClient) Close() error {
	return c.conn.Close()
}

func (c *UserConfigClient) CreateStrategy(ctx context.Context, req *pb.CreateStrategyRequest) (*pb.CreateStrategyResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.CreateStrategy(ctx, req)
}

func (c *UserConfigClient) UpdateStrategy(ctx context.Context, req *pb.UpdateStrategyRequest) (*pb.UpdateStrategyResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.UpdateStrategy(ctx, req)
}

func (c *UserConfigClient) DeleteStrategy(ctx context.Context, req *pb.DeleteStrategyRequest) (*pb.DeleteStrategyResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.DeleteStrategy(ctx, req)
}

func (c *UserConfigClient) GetStrategy(ctx context.Context, req *pb.GetStrategyRequest) (*pb.GetStrategyResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.GetStrategy(ctx, req)
}

func (c *UserConfigClient) ListUserStrategies(ctx context.Context, req *pb.ListUserStrategiesRequest) (*pb.ListUserStrategiesResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.ListUserStrategies(ctx, req)
}

func (c *UserConfigClient) ActivateStrategy(ctx context.Context, req *pb.ActivateStrategyRequest) (*pb.ActivateStrategyResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.ActivateStrategy(ctx, req)
}

func (c *UserConfigClient) DeactivateStrategy(ctx context.Context, req *pb.DeactivateStrategyRequest) (*pb.DeactivateStrategyResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.DeactivateStrategy(ctx, req)
}

func (c *UserConfigClient) HealthCheck(ctx context.Context, req *common.HealthCheckRequest) (*common.HealthCheckResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.client.HealthCheck(ctx, req)
}
