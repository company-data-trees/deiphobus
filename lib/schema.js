var Marshal = require('./marshal.js');

var Schema = function(raw) {
    this.name_types = {};
    this.value_types = {};
    
    if (!raw) return;

    this.default_name_type = raw.default_name_type;
    this.default_value_type = raw.default_value_type;

    var assign = function(types) {
        return function(entry) {
            types[Marshal[raw.default_name_type].from(entry.key)] = entry.value;
        };
    };

    raw.name_types.entrySet().toArray().forEach(assign(this.name_types));
    raw.value_types.entrySet().toArray().forEach(assign(this.value_types));
}

module.exports = Schema;


