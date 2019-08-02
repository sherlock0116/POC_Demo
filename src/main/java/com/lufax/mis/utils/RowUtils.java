package com.lufax.mis.utils;

import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Descriptor:
 * Author: sherlock
 * Date: 2019-08-02 3:38 PM
 */
public class RowUtils {

    /**
     * 将Row转换为Iterator
     * @param row
     * @return
     */
    public static Iterator<Object> toIterable(Row row) {
        List<Object> elements = new ArrayList<>();
        int length = row.getArity();
        if (length <= 0) {
            System.out.println("row did not contain element!");
            return null;
        }
        for (int i = 0; i < length; i++) {
            elements.add(row.getField(i));
        }
        return elements.iterator();
    }
}
