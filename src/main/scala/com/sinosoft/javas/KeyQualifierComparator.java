package com.sinosoft.javas;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

public class KeyQualifierComparator implements Comparator<Tuple2<ImmutableBytesWritable,byte[]>>, Serializable {
    @Override
    public int compare(Tuple2<ImmutableBytesWritable, byte[]> o1, Tuple2<ImmutableBytesWritable, byte[]> o2) {
        if(o1._1().compareTo(o2._1()) == 0){
            return Bytes.compareTo(o1._2(),o2._2());
        }else{
            return  o1._1().compareTo(o2._1());
        }
    }
}
