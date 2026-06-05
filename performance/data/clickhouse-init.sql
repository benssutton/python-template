CREATE TABLE IF NOT EXISTS default.items (
    id    UInt64,
    name  String,
    value String
) ENGINE = MergeTree() ORDER BY id;

INSERT INTO default.items VALUES (1, 'alpha', 'a'), (2, 'beta', 'b'), (3, 'gamma', 'c');
