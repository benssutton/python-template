CREATE TABLE IF NOT EXISTS default.items (
    id    UInt64,
    name  String,
    value String
) ENGINE = MergeTree() ORDER BY id;
