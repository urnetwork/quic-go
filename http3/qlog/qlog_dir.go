package qlog

import (
	"context"

	"github.com/urnetwork/quic-go"
	"github.com/urnetwork/quic-go/qlog"
	"github.com/urnetwork/quic-go/qlogwriter"
)

const EventSchema = "urn:ietf:params:qlog:events:http3-12"

func DefaultConnectionTracer(ctx context.Context, isClient bool, connID quic.ConnectionID) qlogwriter.Trace {
	return qlog.DefaultConnectionTracerWithSchemas(ctx, isClient, connID, []string{qlog.EventSchema, EventSchema})
}
