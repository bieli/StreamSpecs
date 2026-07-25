# StreamSpecs

Real-time **streaming data quality validator** for Apache Kafka, written in **Scala 3** with **Cats Effect 3** and **FS2**.

Validates events *on the stream* (before they land in a warehouse), routes failures to a Dead Letter Queue, and raises stateful alerts (Dead Man's Switch + rolling average).

## License

MIT
