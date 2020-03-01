package com.test.join;

import com.test.groupingcomparator.OrderBean;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class RJComparator extends WritableComparator {

    protected RJComparator() {
        super(TableBean.class, true);
    }


    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TableBean aBean = (TableBean) a;
        TableBean bBean = (TableBean) b;

        return aBean.getP_id().compareTo(bBean.getP_id());
    }
}
