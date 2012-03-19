
var Marshal = require('./marshal.js');

var Row = function(raw, schema) {
    var keyType = schema.name_types['KEY'] || schema.default_name_type;
    this.key = Marshal[keyType].from(raw.key);

    var columns = this.columns = [];

    raw.columns.toArray().forEach(function(column) {
        var name = Marshal[keyType].from(column.name)
        var type = schema.value_types[name] || schema.default_value_type;
        var marshal = Marshal[type];

        columns.push({
            name: name,
            type: type,
            value: marshal ? marshal.from(column.value) : column.value,
            timestamp: new Date(column.timestamp / 1000),
            ttl: column.ttl
        });
    });
}

module.exports = Row;
