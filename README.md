# StreamSpecs

Real-time **streaming data quality validator** for Apache Kafka, written in **Scala 3** with **Cats Effect 3** and **FS2**.

Validates events *on the stream* (before they land in a warehouse), routes failures to a Dead Letter Queue, and raises stateful alerts (Dead Man's Switch + rolling average).

## Quick start (simulation - no Kafka)

Requires **JDK 17+** and **sbt**.

```bash
# if your default Java is older than 17:
./scripts/with-jdk17.sh sbt run

# or simply:
sbt run
```

## CI (GitHub Actions)

On every push / PR to `main` (or `master`):

| Job | Command |
|---|---|
| Format | `sbt scalafmtCheckAll` |
| Compile | `sbt Test/compile` (with `-Werror`) |
| Test | `sbt test` |

Locally mirror CI with:

```bash
sbt ci          # fmtCheck + compile + test
sbt fmt         # rewrite sources with scalafmt
sbt fmtCheck    # check only
```

## Stack

- Scala 3.3
- Cats Effect 3
- FS2 3
- fs2-kafka
- Circe
- PureConfig
- Prometheus
- MUnit

## License

MIT
