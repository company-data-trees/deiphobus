
var Marshal = require('./marshal');

var Keyspace = function(raw) {
    this.name = raw.name;
    this.strategy = new ScriptableMap(raw.strategy_options);
    this.strategy.name = raw.strategy_class.split('org.apache.cassandra.locator.').pop();
    this.durable_writes = raw.durable_writes;

    var cfs = this.column_families = {};
    raw.cf_defs.toArray().forEach(function(raw) {
        var cf = cfs[raw.name] = {
            type: raw.column_type,
            comparator: raw.comparator_type.split('org.apache.cassandra.db.marshal.').pop(),
            default_type: raw.default_validation_class.split('org.apache.cassandra.db.marshal.').pop(),
            key_type: raw.key_validation_class.split('org.apache.cassandra.db.marshal.').pop(),
            comment: raw.comment,
            read_repair_chance: raw.read_repair_chance,
            gc_grace_seconds: raw.gc_grace_seconds,
            replicate_on_write: raw.replicate_on_write,
            merge_shards_chance: raw.merge_shards_chance,
            id: raw.id,
            compression_options: new ScriptableMap(raw.compression_options),
            compaction_threshold: {
                min: raw.min_compaction_threshold,
                max: raw.max_compaction_threshold
            },
            cache: {
                row: {
                    size: raw.row_cache_size,
                    save_period: raw.row_cache_save_period_in_seconds,
                    keys_to_save: raw.row_cache_keys_to_save,
                    provider: raw.row_cache_provider.split('org.apache.cassandra.cache.').pop()
                },
                key: {
                    size: raw.key_cache_size,
                    save_period: raw.key_cache_save_period_in_seconds
                }
            }
        };

        cf.key_alias = Marshal.for(cf.key_validation).from(raw.key_alias);

        if (raw.compaction_strategy) {
            cf.compaction_strategy = new ScriptableMap(raw.compaction_strategy_options);
            cf.compaction_strategy.name = raw.compaction_strategy.split('org.apache.cassandra.db.compaction.').pop();
        }
        
        var marshal = Marshal.for(cf.comparator);
        cf.columns = {};
        raw.column_metadata.toArray().forEach(function(raw) {
            cf.columns[marshal.from(raw.name)] = {
                type: raw.validation_class.split('org.apache.cassandra.db.marshal.').pop()
            };
        });
    });
}

module.exports = Keyspace;
