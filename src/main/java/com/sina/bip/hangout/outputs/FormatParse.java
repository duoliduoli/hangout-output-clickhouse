package com.sina.bip.hangout.outputs;

import java.util.List;
import java.util.Map;
import java.util.Vector;

public interface FormatParse {

    public void bulkInsert(Vector<Map> events) throws Exception;
    public void prepare();
}
