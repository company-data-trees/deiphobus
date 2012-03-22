var setInterval = require('ringo/scheduler').setInterval;

var Connection = require('./connection');

var Pool = function(options) {
    if (!(this instanceof Pool)) {
        return new Pool(options);
    }

    var connections = this.connections = [];

    this.hosts = options.hosts || [];
    if (options.host) this.hosts.unshift(options.host);

    this.hosts.forEach(function(host) {
        connections.push(new Connection(host));
    });
}

Pool.prototype.acquire = sync(function() {
    return this.connections.shift();
});

Pool.prototype.release = sync(function(connection) {
    this.connections.push(connection);
});

Pool.prototype.do = function(action) {
    try {
        var connection = this.acquire();
        return action(connection);
    } finally {
        this.release(connection);
    }
}

Pool.prototype.refresh = function() {
    return this.do(function(connection) {
        return connection.execute('describe_ring', this.keyspace);
    }.bind(this));
}

Pool.prototype.use = sync(function(keyspace) {
    keyspace = this.keyspace = this.connections[0].use(keyspace);
    this.connections.slice(1).forEach(function(connection) {
        connection.use(keyspace);
    });
});

Pool.prototype.execute = function() {
    var args = Array.prototype.slice.apply(arguments);
    return this.do(function(connection) {
        return connection.execute.apply(connection, args);
    });
}

Pool.prototype.cql = function() {
    var args = Array.prototype.slice.apply(arguments);
    return this.do(function(connection) {
        return connection.cql.apply(connection, args);
    });
}

module.exports = Pool;
