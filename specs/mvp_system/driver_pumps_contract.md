# Driver And Pump Contract

This document defines the behavioral contract for the iroh driver, edge demux,
and send/recv pumps. The driver owns tokio, connections, streams, demux, and
byte-pump tasks.

## Driver Authority

- The driver owns one iroh endpoint.
- The driver owns connection caching.
- The driver owns edge ALPN.
- The driver owns edge-id stream demux.
- The driver owns send and recv pump tasks.
- Actors do not poll stream futures directly in the MVP.

## Connection And Stream Shape

- Each edge uses one persistent uni-stream.
- The stream starts with an `edge_id` preamble.
- After the preamble, the stream carries object records as bytes.
- The stream is not opened per object.
- Connections may be cached per `(peer_node_id, ALPN)`.

## Receive Rendezvous

- `EstablishRecv` may arrive before the stream.
- The stream may arrive before `EstablishRecv`.
- The driver stores whichever half arrives first.
- The recv pump starts only after both receive spec and stream exist.
- A pending stream is not read before the receive spec exists.

## Recv Pump

- The recv pump is byte-blind after edge demux.
- It does not parse `ObjectHeader`.
- It copies QUIC bytes into free ingress ring spans.
- It advances `commit` after bytes are in the ring.
- It emits or coalesces `RingReadable`.
- If no ring space exists, it stops reading and waits for `RingWritable`.

## Send Pump

- The send pump opens one uni-stream for the edge.
- It writes the `edge_id` preamble once.
- It writes committed egress ring bytes to QUIC.
- It advances `consume` only after `write_all` accepts bytes.
- It emits or coalesces `RingWritable`.
- If network or QUIC flow control stalls, it keeps ownership of unread ring
  bytes until the write completes.

## Fault And Stop

- Read error emits `StreamFault`.
- Write error emits `StreamFault`.
- Protocol edge failure emits `StreamFault`.
- `StopEdge` stops the corresponding pump.
- Stopped pumps emit `PumpStopped`.

## Test Direction

Tests should use mock streams and rings to drive driver messages and pump wake
events. Successful tests should assert rendezvous in both arrival orders,
preamble once, byte-blind copying, cursor advancement after I/O acceptance, and
backpressure. Fault tests should inject read/write errors and stop races.
