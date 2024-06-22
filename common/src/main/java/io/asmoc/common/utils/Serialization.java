package io.asmoc.common.utils;

import io.asmoc.common.algorithm.rbm.RoaringBitMap;

import java.io.*;
import java.util.Base64;

public final class Serialization {

    public static byte[] serialize(Object obj) throws IOException {

        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        ObjectOutputStream objOut = new ObjectOutputStream(byteOut);
        objOut.writeObject(obj);
        objOut.close();
        return byteOut.toByteArray();

    }

    public static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
        ObjectInputStream objIn = new ObjectInputStream(byteIn);
        Object obj = objIn.readObject();
        objIn.close();
        return obj;
    }

    public static String byteToStr(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }

    public static byte[] strToByte(String base64String) {
        return Base64.getDecoder().decode(base64String);
    }

    public static void main(String[] args) {
        try {
            // 创建一个示例对象
//            RoaringBitMap r = new RoaringBitMap();
//            r.add(123123123);
//
//            // 序列化对象
//            byte[] serializedBytes = Serialization.serialize(r);
//
//            // 将字节数组转换为字符串
//            String encodedString = Serialization.byteToStr(serializedBytes);
//
//            // 打印Base64编码的字符串
//            System.out.println("Base64 Encoded String: " + encodedString);

            String encodedString = "rO0ABXNyACtpby5hc21vYy5jb21tb24uYWxnb3JpdGhtLnJibS5Sb2FyaW5nQml0TWFwAAAAAAAAAAECAAFMAANyYm10ACFMb3JnL3JvYXJpbmdiaXRtYXAvUm9hcmluZ0JpdG1hcDt4cHNyAB9vcmcucm9hcmluZ2JpdG1hcC5Sb2FyaW5nQml0bWFwAAAAAAAAAAYMAAB4cHcWOjAAAAEAAACKAAIAEAAAABC8EbwTvHg=";

            RoaringBitMap nr = RoaringBitMap.getRbm(encodedString);

            // 打印反序列化的对象
            System.out.println("Deserialized Object: " + nr.getCardinality());

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

}
