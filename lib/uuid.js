
var TimeUUID = com.eaio.uuid.UUID;
var JavaUUID = java.util.UUID;
var ByteBuffer = java.nio.ByteBuffer;

module.exports = {
    now: function() {
        var t = new TimeUUID();
        return new JavaUUID(t.getTime(), t.getClockSeqAndNode());
    },

    for: function(time, max) {
        var ts = time - Date.UTC(1582, 9, 15);
        var hm = ((ts / 0x100000000) * 10000) & 0xFFFFFFF;
        var low = ((ts & 0xFFFFFFF) * 10000) % 0x100000000;
        var mid = hm & 0xFFFF;
        var hi = hm >>> 16;
        var thav = (hi & 0xFFF) | 0x1000;  // set version '0001'

        if (low > java.lang.Integer.MAX_VALUE)
            low = java.lang.Integer.MIN_VALUE + (low - java.lang.Integer.MAX_VALUE) - 1;
        if (mid > java.lang.Short.MAX_VALUE)
            mid = java.lang.Short.MIN_VALUE + (mid - java.lang.Short.MAX_VALUE) - 1;
        if (thav > java.lang.Short.MAX_VALUE)
            thav = java.lang.Short.MIN_VALUE + (thav - java.lang.Short.MAX_VALUE) - 1;

        var msb = ByteBuffer.allocate(8)
            .putInt(low)
            .putShort(mid)
            .putShort(thav)
            .rewind();

        return JavaUUID(msb.getLong(), max ? -1 : 0);
    },

    toByteBuffer: function(uuid) {
        if (!(uuid instanceof JavaUUID)) {
            uuid = JavaUUID.fromString(uuid);
        }

        return ByteBuffer.allocate(16)
            .putLong(uuid.getMostSignificantBits())
            .putLong(uuid.getLeastSignificantBits())
            .rewind();
    },

    toByteArray: function(uuid) {
        return this.toByteBuffer(uuid).array();
    },

    fromByteBuffer: function(buffer) {
        return new JavaUUID(buffer.getLong(), buffer.getLong());
    },

    fromByteArray: function(bytes) {
        return this.fromByteBuffer(ByteBuffer.wrap(bytes));
    }
};
