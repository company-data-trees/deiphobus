
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

// connection helpers
Selector.prototype.do = function(callback) {
    return this.connection.do(callback.bind(this));
}

Selector.prototype.execute = function() {
    var args = Array.prototype.slice.apply(arguments);
    return this.do(function(c) {
        return c.execute.apply(c, args);
    });
}

// internal methods for creating various Cassandra Thrift objects
// (e.g. column slices, key ranges, etc)
var EMPTY = ByteBuffer.allocate(0);

function tokenRange(start, end) {
    var range = new KeyRange();
    range.setStart_token(start);
    range.setEnd_token(end);
    return range;
}

function specificColumns(columns, marshal) {
    marshal = marshal || Marshal.for('UTF8Type');
    var predicate = new SlicePredicate();
    columns.forEach(function(column) {
        predicate.addToColumn_names(marshal.to(column));
    });
    return predicate;
}

var columnRange = function(options) {
    options = options || {};
    var marshal = options.type || Marshal.for('UTF8Type');
    var reversed = options.revered || false;
    var limit = options.limit || java.lang.Integer.MAX_VALUE;

    var predicate = new SlicePredicate();

    var mStart = (typeof options.start == 'undefined') ? EMPTY : marshal.to(options.start);
    var mEnd = (typeof options.end == 'undefined') ? EMPTY : marshal.to(options.end);

    var range = new SliceRange(mStart, mEnd, reversed, limit);
    predicate.setSlice_range(range);
    return predicate;
}

var allColumns = columnRange();

Selector.prototype.splits = function(column_family) {
    return this.do(function(c) {
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

Selector.prototype.get = function(column_family, keys, options) {
    var parent = new ColumnParent(column_family);
    var columns = allColumns;
    var consistency = ConsistencyLevel.ONE;

    options = options || {};
    if (options.consistency) {
        consistency = ConsistencyLevel[options.consistency.toUpperCase()];
    }

    var schema = this.do(function(c) { return c.keyspace.column_families[column_family]; });
    var marshalKey = Marshal.for(schema.key_type);
    var marshalName = Marshal.for(options.comparator || schema.comparator);

    if (options.columns instanceof Array || options.column instanceof Array) {
        columns = specificColumns(options.columns || options.column);
    } else if (typeof options.column == 'object' || typeof options.columns == 'object') {
        columns = columnRange(options.column || options.columns);
    } else if (options.column) {
        columns = specificColumns([options.column]);
    }
    
    var rows;
    if (keys instanceof Array) {
        var keyList = new java.util.ArrayList();
        keys.forEach(function(key) {
            keyList.add(marshalKey.to(key));
        });
        rows = [];
        entries = this.execute('multiget_slice', keyList, parent, columns, consistency).entrySet().iterator();
        while (entries.hasNext()) {
            var entry = entries.next();
            rows.push({
                key: entry.getKey(),
                columns: entry.getValue()
            });
        }
    } else if (typeof keys == 'object') {
        var range = tokenRange(keys.start, keys.end);
        rows = this.execute('get_range_slices', parent, columns, range, consistency);
    } else if (typeof keys == 'undefined') {
        var range = tokenRange('0', '0');
        rows = this.execute('get_range_slices', parent, columns, range, consistency);
    } else {
        var key = marshalKey.to(keys);
        rows = [{
            key: key,
            columns: this.execute('get_slice', key, parent, columns, consistency)
        }];
    }

    var rows = new ScriptableList(rows);

    if (rows.length == 0) return [];

    var lastKey = rows[rows.length - 1].key.duplicate();

    var result = rows.map(function(raw) {
        var row = {
            key: marshalKey.from(raw.key),
            columns: {}
        };

        (new ScriptableList(raw.columns)).forEach(function(raw) {
            var column = raw.column || raw.counter_column;

            var name = marshalName.from(column.name);
            var nameString = name;
            if (Array.isArray(name)) nameString = name.join(':');

            var columnSchema = schema.columns[nameString];

            var type = columnSchema ? columnSchema.type : schema.default_type;

            var timestamp = column.timestamp === undefined ? undefined :
                new Date(column.timestamp / 1000);

            row.columns[nameString] = {
                name: name,
                type: type,
                value: Marshal.for(type).from(column.value),
                timestamp: timestamp,
                ttl: column.ttl
            };
        });

        return row;
    });

    result.lastKey = lastKey;
    return result;
}

Selector.prototype.iterate = function(column_family, options) {
    var partitioner = new Packages[this.execute('describe_partitioner')];

    options = options || {};

    // extract token range. if not specified, iterate through all rows.
    var startToken = options.start || '0';
    var endToken = options.end || '0';

    // remove token range parameters from options hash
    delete options.start;
    delete options.end;

    while (true) {
        var rows = this.get(column_family, {start: startToken, end: endToken}, options);
        if (rows.length == 0) break;

        for(var i = 0; i < rows.length; i++) {
            yield(rows[i]);
        }

        var lastToken = partitioner.getToken(rows.lastKey).toString();
        if (lastToken == endToken) break;

        startToken = lastToken;
    }

    yield(null);
}

module.exports = Selector;
