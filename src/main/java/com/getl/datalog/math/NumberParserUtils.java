package com.getl.datalog.math;

import cn.hutool.core.util.NumberUtil;

public class NumberParserUtils {
    public static double parse(String str) {
        return NumberUtil.parseDouble(str, 0D);
    }
}
