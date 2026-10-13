# BRB service

`brb.proto` defines `Bracha.Propose`, `Bracha.Echo` and `Bracha.Ready`.
Each is a Gorums one-way multicast. The generated `.Send()` method waits for
sends, not application delivery.

Regenerate with `make generate`. Do not edit generated Go files.
All replicas must use the same generated RPC names. Old binaries using
`Broadcast.Send` are incompatible with this version.

A broadcast ID contains the origin and sequence. The logical sender field is
trusted until upstream authenticated peer verification is integrated.
