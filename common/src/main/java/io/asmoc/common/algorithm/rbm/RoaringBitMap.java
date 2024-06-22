package io.asmoc.common.algorithm.rbm;

import io.asmoc.common.utils.Serialization;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.io.Serializable;

public class RoaringBitMap implements Serializable {

    private static final long serialVersionUID = 1L; // 序列化版本号，用于控制版本兼容性

    private final RoaringBitmap rbm;

    public RoaringBitMap() {
        rbm = new RoaringBitmap();
    }

    public void add(int item) {
        rbm.add(item);
    }

    public void remove(int item) {
        rbm.remove(item);
    }

    public RoaringBitmap get() {
        return this.rbm;
    }

    public String getRbmStr() throws IOException {
        byte[] rbmBytes = Serialization.serialize(this);
        return Serialization.byteToStr(rbmBytes);
    }

    public static RoaringBitMap getRbm(String rbmStr) throws IOException, ClassNotFoundException {
        byte[] rbmBytes = Serialization.strToByte(rbmStr);
        return (RoaringBitMap) Serialization.deserialize(rbmBytes);
    }

    public void merge(RoaringBitMap rbm) {
        this.rbm.or(rbm.get());
    }

    public int getCardinality() {
        return rbm.getCardinality();
    }

}
