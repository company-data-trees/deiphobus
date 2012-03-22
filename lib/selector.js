
var {ConsistencyLevel, ColumnParent, KeyRange, SlicePredicate, SliceRange}
    = org.apache.cassandra.thrift;
var ByteBuffer = java.nio.ByteBuffer;

var Marshal = require('./marshal');

var Selector = function(connection) {
    if (!(this instanceof Selector)) {
        return new Selector(connection);
    }

    this.connection = connection;
}

var EMPTY = ByteBuffer.allocate(0);

var allColumns = function() {
    var predicate = new SlicePredicate();
    var range = new SliceRange(EMPTY, EMPTY, false, java.lang.Integer.MAX_VALUE);
    predicate.setSlice_range(range);
    return predicate;
}();

function keyTokenRange(start, end) {
    var range = new KeyRange(20);
    range.setStart_token(start);
    range.setEnd_token(end);
    return range;
}

Selector.prototype.splits = function(column_family) {
    return this.connection.do(function(c) {
        var splits = [];
        c.execute('describe_ring', c.keyspace.name).toArray().forEach(function(range) {
            var tokens = c.execute('describe_splits', column_family, range.start_token, range.end_token, 100).toArray();
            for (var i = 1; i < tokens.length; i++) {
                splits.push({
                    start: tokens[i - 1],
                    end: tokens[i],
                    endpoints: new ScriptableList(range.rpc_endpoints || range.endpoints)
                });
            }
        });
        return splits;
    });
}

Selector.prototype.rows = function(column_family, split) {
    var keyRange = keyTokenRange(split.start, split.end);
    return this.connection.do(function(c) {
        var rows = new ScriptableList(c.execute('get_range_slices',
            new ColumnParent(column_family),
            allColumns, keyRange, ConsistencyLevel.ONE
        ));

        if (rows.length == 0) return [];

        var schema = c.keyspace.column_families[column_family];
        var marshalKey = Marshal.for(schema.key_type);
        var marshalName = Marshal.for(schema.comparator);
        var lastKey = rows[rows.length - 1].key.duplicate();

        var result = rows.map(function(raw) {
            var row = {
                key: marshalKey.from(raw.key),
                columns: {}
            };

            (new ScriptableList(raw.columns)).forEach(function(raw) {
                var name = marshalName.from(raw.column.name);
                var type = schema.columns[name].type;
                row.columns[name] = {
                    type: type,
                    value: Marshal.for(type).from(raw.column.value),
                    timestamp: new Date(raw.column.timestamp / 1000),
                    ttl: raw.column.ttl
                };
            });

            return row;
        });

        result.lastKey = lastKey;
        return result;
    });
}

Selector.prototype.iterate = function(column_family, split) {
    var partitioner = this.connection.do(function(c) {
        return new Packages[c.execute('describe_partitioner')];
    });

    var startToken = split.start;
    var endToken = split.end;

    while (true) {
        var rows = this.rows(column_family, {start: startToken, end: endToken});
        if (rows.length == 0) break;

        for(var i = 0; i < rows.length; i++) {
            yield(rows[i]);
            // if (Object.keys(rows[i].columns).length > 0) yield(rows[i]);
        }

        var lastToken = partitioner.getToken(rows.lastKey).toString();
        if (lastToken == endToken) break;

        startToken = lastToken;
    }

    yield(null);
}

module.exports = Selector;
