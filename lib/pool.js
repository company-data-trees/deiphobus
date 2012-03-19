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
    }
}

Pool.acquire = sync(function() {
    return this.connections.shift();
});

Pool.release = sync(function(connection) {
    this.connections.push(connection);
});

Pool.prototype.refresh = function() {
    var connection = this.acquire();
    var endpoints = connection.client.describe_ring(this.keyspace);
    this.release(connection);
    return endpoints;
}

Pool.prototype.use = function(keyspace) {
    this.keyspace = keyspace;
    this.connections.forEach(function(connection) {
        connection.use(keyspace);
    });
}

module.exports = Pool;
