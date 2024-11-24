package io.asmoc.common.algorithm.theta;

import java.io.Serializable;

import org.apache.datasketches.theta.*;


public class ThetaSketch implements Serializable {

    private UpdateSketch sketch;

    // 构造函数，允许用户指定K值来控制精度
    public ThetaSketch(int k) {
        sketch = UpdateSketch.builder().setNominalEntries(k).build();
    }

    // 插入元素到Sketch中
    public void update(String item) {
        sketch.update(item);
    }

    // 合并另一个ThetaSketch
    public void merge(ThetaSketch otherSketch) {
        Union union = SetOperation.builder().buildUnion();
        CompactSketch compactSketch = union.union(this.sketch, otherSketch.getSketch());
        HashIterator it = compactSketch.iterator();
        this.sketch.reset();
        while (it.next()) {
            this.sketch.update(it.get());
        }

    }

    // 返回估算结果
    public double getEstimate() {
        CompactSketch compactSketch = sketch.compact();
        return compactSketch.getEstimate();
    }

    // 返回内部Sketch对象
    private UpdateSketch getSketch() {
        return this.sketch;
    }

    // 重置Sketch
    public void reset() {
        this.sketch.reset();
    }

    public static void main(String[] args) {
        // 示例使用
        ThetaSketch sketch1 = new ThetaSketch(4096); // k值为4096，越高精度越高
        sketch1.update("a");
        sketch1.update("b");
        sketch1.update("c");

        System.out.println("Estimated1 cardinality: " + sketch1.getEstimate());

        ThetaSketch sketch2 = new ThetaSketch(4096);
        sketch2.update("b");
        sketch2.update("c");
        sketch2.update("d");

        System.out.println("Estimated2 cardinality: " + sketch2.getEstimate());

        // 合并两个Sketch
        sketch1.merge(sketch2);

        // 输出估算的基数
        System.out.println("Estimated cardinality: " + sketch1.getEstimate());
    }
}
