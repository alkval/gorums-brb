# BRB service

`brb.proto` defines `Bracha.Propose`, `Bracha.Echo` and `Bracha.Ready`.
`Bracha.QEcho` is used only by the experimental optimistic variant. The message
marks the selected variant, and receivers reject mismatched variants.
Each is a Gorums one-way multicast. The generated `.Send()` method waits for
sends, not application delivery.

Regenerate with `make generate`. It builds the Go and Gorums generators from
the module versions in `go.mod`, using the `protoc` compiler on PATH.
Do not edit generated Go files.
All replicas must use the same generated RPC names. Old binaries using
`Broadcast.Send` are incompatible with this version.

A broadcast ID contains the origin and sequence. There is no sender field:
receivers use the peer ID that Gorums reports for the stream. Field number 3 and
the name `sender` are reserved to prevent reuse. Mixed-version groups are
unsupported. Restart the whole group with matching binaries.
