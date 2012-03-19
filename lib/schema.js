var Marshal = require('./marshal.js');

var Schema = function(raw) {
    this.default_name_type = raw.default_name_type;
    this.default_value_type = raw.default_value_type;

    var types = this.name_types = {};
    var assign = function(entry) {
        types[Marshal[raw.default_name_type].from(entry.key)] = entry.value;
    };

    raw.name_types.entrySet().toArray().forEach(assign);

    var types = this.value_types = {};
    raw.value_types.entrySet().toArray().forEach(assign);
}

module.exports = Schema;


