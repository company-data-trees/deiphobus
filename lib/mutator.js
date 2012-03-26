
var {ConsistencyLevel, ColumnParent, ColumnOrSuperColumn, Column, Mutation}
    = org.apache.cassandra.thrift;

var Marshal = require('./marshal');

var Mutator = function(connection) {
    if (!(this instanceof Mutator)) {
        return new Mutator(connection);
    }

    this.connection = connection;
}

// connection helpers
Mutator.prototype.do = function(callback) {
    return this.connection.do(callback.bind(this));
}

Mutator.prototype.execute = function() {
    var args = Array.prototype.slice.apply(arguments);
    return this.do(function(c) {
        return c.execute.apply(c, args);
    });
}

Mutator.prototype.put = function(column_family, key, columns, options) {
    var consistency = ConsistencyLevel.ONE;

    options = options || {};
    if (options.consistency) {
        consistency = ConsistencyLevel[options.consistency.toUpperCase()];
    }

    var schema = this.do(function(c) { return c.keyspace.column_families[column_family]; });
    var marshalKey = Marshal.for(schema.key_type);
    var marshalName = Marshal.for(schema.comparator);
    var keyBytes = marshalKey.to(key);

    if (!(columns instanceof Array)) {
        columns = Object.keys(columns).map(function(key) {
            return {
                name: key,
                value: columns[key]
            };
        });
    }

    var timestamp = Date.now() * 1000;
    var mutations = new java.util.ArrayList();
    columns.forEach(function(column) {
        var c = new Column();
        c.setName(marshalName.to(column.name));

        var columnSchema = schema.columns[column.name];
        var type = columnSchema ? columnSchema.type : schema.default_type;
        c.setValue(Marshal.for(type).to(column.value));

        if (column.ttl) c.setTtl(column.ttl);
        c.setTimestamp(timestamp);

        var csc = new ColumnOrSuperColumn();
        csc.setColumn(c);
        var m = new Mutation();
        m.setColumn_or_supercolumn(csc);
        mutations.add(m);
    });

    var cfMap = new java.util.HashMap();
    cfMap.put(column_family, mutations);

    var map = new java.util.HashMap();
    map.put(keyBytes, cfMap);

    this.execute('batch_mutate', map, consistency);
}

module.exports = Mutator;
