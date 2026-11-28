package qlog

import (
	"context"

	"github.com/urnetwork/quic-go/v2026"
	"github.com/urnetwork/quic-go/v2026/qlog"
	"github.com/urnetwork/quic-go/v2026/qlogwriter"
)

const EventSchema = "urn:ietf:params:qlog:events:http3-12"

func DefaultConnectionTracer(ctx context.Context, isClient bool, connID quic.ConnectionID) qlogwriter.Trace {
	return qlog.DefaultConnectionTracerWithSchemas(ctx, isClient, connID, []string{qlog.EventSchema, EventSchema})
}
