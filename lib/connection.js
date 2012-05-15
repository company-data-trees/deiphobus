
var {TSocket, TFramedTransport} = org.apache.thrift.transport;
var {TBinaryProtocol} = org.apache.thrift.protocol;
var {Cassandra, Compression, CqlResultType} = org.apache.cassandra.thrift;

var {format} = require('ringo/utils/strings');

var Schema = require('./schema');
var Row = require('./row');
var Keyspace = require('./keyspace');

var Connection = function(options) {
    if (!(this instanceof Connection)) {
        return new Connection(options);
    }

    options = options || {};

    if (!options.port && options.host && options.host.indexOf(':') > -1) {
        [options.host, options.port] = options.host.split(':');
    }

    this.port = options.port || 9160;
    this.host = options.host || 'localhost';

    var socket = new TSocket(this.host, this.port);
    socket.open();

    var transport = new TFramedTransport(socket)
    var protocol = new TBinaryProtocol(transport);
    this.client = new Cassandra.Client(protocol);

    this.alive = true;
}

Connection.prototype.use = function(keyspace) {
    if (typeof keyspace == 'string')
        keyspace = new Keyspace(this.execute('describe_keyspace', keyspace));
    
    this.keyspace = keyspace;
    this.client.set_keyspace(this.keyspace.name);
    return this.keyspace;
}

Connection.prototype.execute = function() {
    var args = Array.prototype.slice.apply(arguments),
        command = args.shift();

    return this.client[command].apply(this.client, args);
}

Connection.prototype.cql = function() {
    var args = Array.prototype.slice.apply(arguments),
        query = args.shift();

    var escaped = args.map(function(arg) {
        if (typeof arg == 'string')
            return "'" + arg.replace(/'/g, "''") + "'";
        return arg;
    });

    escaped.unshift(query);
    var cql = format.apply(null, escaped);
    var buffer = java.nio.ByteBuffer.wrap(cql.toByteArray());

    // TODO: find a zlib/gzip jar so we can support compression...
    var result = this.execute('execute_cql_query', buffer, Compression.NONE);

    if (result.type == CqlResultType.ROWS) {
        var schema = new Schema(result.schema);
        schema.raw = result.schema;
        var rows = (new ScriptableList(result.rows)).map(function(row) {
            return new Row(row, schema);
        });
        rows.schema = schema;
        rows.raw = result;
        return rows;
    }

    if (result.type == CqlResultType.INT) return result.num;
}

// this mirrors the Pool interface, so code can be written
// to transparently work with either a connection or a pool.
Connection.prototype.do = function(action) {
    return action(this);
}

module.exports = Connection;
