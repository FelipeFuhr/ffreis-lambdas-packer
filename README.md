## lambdas-packer

This repo provides `lambdas-packer`, a Go CLI that:

- zips artifacts that are not zipped yet (e.g. `*/bootstrap` -> zip streamed to S3)
- syncs artifacts into an S3 “folder” (prefix), optionally deleting extra `.zip` keys under that prefix

### Usage

Build your lambdas (example from `ffreis-website-lambdas-rust2`):

```bash
make package
```

Then sync artifacts to S3 (true sync under the prefix: uploads + deletes extra `.zip` keys):

```bash
go run ./cmd/lambdas-packer \
  --bucket my-bucket \
  --prefix lambdas/dev/ \
  --artifact-dir /path/to/repo/lambdas/target/lambda
```

Use `--dry-run` to preview changes or `--no-delete` to only upload/update.
