# Development workflow

Open an issue describing the problem before starting new work. Keep one concern
per branch and pull request. Use a descriptive branch name with the issue number,
for example `feature/12-default-fault-bound`.

Run `make fix fmt test vet lint` and `go test -race ./...` before review.
Review the changes produced by `go fix`. Generate bindings after proto changes.

Keep commits focused, with short imperative subjects such as `Bound retained
broadcast state`. Explain the reason in the body and reference the issue with
`Refs: #N`, or `Closes #N` when the change fully resolves it.

Push the branch and open a pull request. Wait for CI and review, then use
GitHub's **Rebase and merge** option. New work should go through this workflow
rather than being pushed directly to `main`.

The report and private project notes do not belong in the public repository.
Never include private host inventories, SSH configuration or credentials.
