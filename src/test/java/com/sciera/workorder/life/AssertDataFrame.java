package com.sciera.workorder.life;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.spark.sql.DataFrame;

public class AssertDataFrame {
    public DataFrame actualDf;
    
    public AssertDataFrame(DataFrame df){
        this.actualDf = df;
    }
    
    public AssertDataFrame hasRows(int numRows){
        assertEquals(numRows, actualDf.count());
        return this;
    }    
    
    public AssertDataFrame containsRowWhere(String whereClause){
        assertNotNull("Did not find row where "+whereClause,actualDf.where(whereClause).first());
        return this;
    }
    
    public AssertDataFrame containsRowWhere(String queryFormat, Object... params){
        containsRowWhere(String.format(queryFormat, (Object[])params));
        return this;
    }
}
