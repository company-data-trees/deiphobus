
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

Selector.prototype.keyTokenRange = function(start, end) {
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

Selector.prototype.names = function(results) {
    return results.toArray().map(function(r) {
        var position = r.key.position();
        var name = Marshal.for('UTF8Type').from(r.key);
        r.key.position(position);
        return name;
    });
}

Selector.prototype.rows = function(column_family, split) {
    var keyRange = this.keyTokenRange(split.start, split.end);
    return this.connection.do(function(c) {
        return c.execute('get_range_slices',
            new ColumnParent(column_family),
            allColumns, keyRange, ConsistencyLevel.ONE
        );
    });
}

Selector.prototype.rows_k = function(column_family, split, last) {
    var keyRange = new KeyRange(10);
    keyRange.setStart_key(last.key);
    keyRange.setEnd_token(split.end);

    return this.connection.do(function(c) {
        return c.execute('get_range_slices',
            new ColumnParent(column_family),
            allColumns, keyRange, ConsistencyLevel.ONE
        );
    });
}


module.exports = Selector;
