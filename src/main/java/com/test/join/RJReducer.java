package com.test.join;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RJReducer extends Reducer<TableBean,NullWritable,TableBean,NullWritable> {
    @Override
    protected void reduce(TableBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        String pname = key.getPname();
        Iterator<NullWritable> iterator = values.iterator();
        iterator.next();
        while (iterator.hasNext()) {
            iterator.next();
            key.setPname(pname);
            context.write(key,NullWritable.get());
        }

//        List<TableBean> orderList = new ArrayList<>();
//        List<TableBean> pdList = new ArrayList<>();
//        try{
//            Iterator<NullWritable> iterator = values.iterator();
//            while (iterator.hasNext()){
//                iterator.next();
//                TableBean tableBean = new TableBean();
//                BeanUtils.copyProperties(tableBean,key);
//                if(StringUtils.isNotEmpty(key.getOrder_id())){
//                    orderList.add(tableBean);
//                }else{
//                    pdList.add(tableBean);
//                }
//            }
//            for(TableBean orderBean : orderList){
//                for(TableBean pdBean : pdList){
//                    if(orderBean.getP_id().equals(pdBean.getP_id())){
//                        orderBean.setPname(pdBean.getPname());
//                    }
//                }
//            }
//
//            for(TableBean orderBean : orderList){
//                context.write(orderBean,NullWritable.get());
//            }
//        }catch (Exception e){
//
//        }

    }
}
