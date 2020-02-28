package com.test.groupingcomparator;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {
    private int order_id; // 订单id号
    private double price; // 价格
    private String productId;
    @Override
    public int compareTo(OrderBean o) {
        int result;

        if (order_id > o.getOrder_id()) {
            result = 1;
        } else if (order_id < o.getOrder_id()) {
            result = -1;
        } else {
            // 价格倒序排序
            result = price > o.getPrice() ? -1 : 1;
        }
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(order_id);
        out.writeUTF(productId);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        order_id = in.readInt();
        productId = in.readUTF();
        price = in.readDouble();
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("OrderBean{");
        sb.append("order_id=").append(order_id);
        sb.append(", price=").append(price);
        sb.append(", productId='").append(productId).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public int getOrder_id() {
        return order_id;
    }

    public void setOrder_id(int order_id) {
        this.order_id = order_id;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }
}
