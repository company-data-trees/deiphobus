
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

var columnRange = function(start, end, options) {
    options = options || {};
    var marshal = options.type || Marshal.for('UTF8Type');
    var reversed = options.revered || false;
    var limit = options.limit || java.lang.Integer.MAX_VALUE;

    var predicate = new SlicePredicate();

    var mStart = (typeof start == 'undefined') ? EMPTY : marshal.to(start);
    var mEnd = (typeof end == 'undefined') ? EMPTY : marshal.to(end);

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
    var marshalName = Marshal.for(schema.comparator);

    if (options.columns instanceof Array || options.column instanceof Array) {
        columns = specificColumns(options.columns || options.column);
    } else if (typeof options.column == 'object' || typeof options.columns == 'object') {
        var c = options.column || options.columns;
        columns = columnRange(c.start, c.end. c.reversed, c.limit);
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
            var name = marshalName.from(raw.column.name);
            var nameString = name;
            if (typeof nameString != 'string') nameString = name.join(':');
            var columnSchema = schema.columns[name];
            var type = columnSchema ? columnSchema.type : schema.default_type;
            row.columns[nameString] = {
                name: name,
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
}

Selector.prototype.iterate = function(column_family, split) {
    var partitioner = new Packages[this.execute('describe_partitioner')];

    split = split || {start: '0', end: '0'}; // iterate through all rows
    var startToken = split.start;
    var endToken = split.end;

    while (true) {
        var rows = this.get(column_family, {start: startToken, end: endToken});
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
