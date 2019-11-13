package com.hbase.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.InvalidProtocolBufferException;

public class TimestampComparator extends ByteArrayComparable {
	public static final Log LOG = LogFactory.getLog(TimestampComparator.class);
	 
    private byte[] data;
 
    //跳过字节数
    private int skip;
 
 
    public TimestampComparator(byte[] data) {
        super(data);
        this.data = data;
    }
 
    /**
     * @param data 目标时间戳
     * @param skip 跳过字节数
     */
    public TimestampComparator(byte[] data, int skip) {
        super(data);
        this.data = data;
        this.skip = skip;
    }
 
    @Override
    public byte[] toByteArray() {
        TimestampBitProtos.TimestampBitComparator.Builder builder = TimestampBitProtos.TimestampBitComparator.newBuilder();
        builder.setData(Bytes.toString(this.data));
        builder.setSkip(this.skip);
        return builder.build().toByteArray();
    }
 
    public static TimestampComparator parseFrom(byte[] pbBytes) throws DeserializationException {
        TimestampBitProtos.TimestampBitComparator prop = null;
        try {
            prop = TimestampBitProtos.TimestampBitComparator.parseFrom(pbBytes);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return new TimestampComparator(Bytes.toBytes(prop.getData()), prop.getSkip());
    }
 
    @Override
    public int compareTo(byte[] bytes, int offset, int len) {
 
        if (len < 13) {
            return 1;
        } else {
            /*byte转为long比较*/
            /*long timeTarget = Long.parseLong(new String(data));
            long timeInDB = Long.parseLong(new String(bytes, skip + offset, 13));
            if (timeTarget < timeInDB) {
                return -1;
            } else if (timeTarget == timeInDB) {
                return 0;
            } else {
                return 1;
            }*/
 
            for (int i = 0; i < data.length; i++) {
                byte b1 = data[i];
                byte b2 = bytes[i + offset + skip];
                if (b1 < b2) {
                    return -1;
                } else if (b1 > b2) {
                    return 1;
                }
            }
            return 0;
        }
    }


}
