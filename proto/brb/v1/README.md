# BRB protocol definitions

`brb.proto` defines one-way Gorums multicast calls for the three phases of
the provisional Bracha-style protocol: `Send`, `Echo`, and `Ready`.
The adapter sends each phase to every process, including itself. The Empty
return type is required by the service definition, but multicast sends no reply.
Successful sending does not mean that recipients processed or delivered the
value. This prototype does not retry failed sends.

Regenerate the Go and Gorums bindings from the repository root:

```sh
make generate
```

The broadcast identifier currently consists of the designated origin process
and an origin-local sequence number. Protocol thresholds and the final system
model remain subject to supervisor confirmation. The prototype carries a trusted
logical sender ID in each phase message. Authentication is deferred, so
experiments must not allow a faulty process to impersonate another process.
