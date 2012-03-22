
var Marshal = require('./marshal.js');

var Row = function(raw, schema) {
    var keyType = schema.name_types['KEY'] || schema.default_name_type;
    this.key = Marshal.for(keyType).from(raw.key);

    var columns = this.columns = {};

    (new ScriptableList(raw.columns)).forEach(function(column) {
        var name = Marshal.for(keyType).from(column.name)
        var type = schema.value_types[name] || schema.default_value_type;
        var marshal = Marshal.for(type);

        columns[name] = {
            type: type,
            value: marshal.from(column.value),
            timestamp: new Date(column.timestamp / 1000),
            ttl: column.ttl
        };
    });
}

module.exports = Row;
