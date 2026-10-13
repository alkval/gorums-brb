# Development workflow

Open an issue describing the problem before starting new work. Keep one concern
per branch and pull request. Use a descriptive branch name with the issue number,
for example `feature/12-default-fault-bound`.

Run `make fix fmt test vet lint` and `go test -race ./...` before review.
Review the changes produced by `go fix`. Generate bindings after proto changes.

Keep commits focused, with short imperative subjects such as `Bound retained
broadcast state`. Rebase is supported. Squash merging is optional.

The report and private project notes do not belong in the public repository.
Never include private host inventories, SSH configuration or credentials.
