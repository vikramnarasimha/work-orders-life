package com.sciera.workorder.life;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Fixture {
    public JavaSparkContext sc;
    public SQLContext sqlContext;
    
    public Fixture(){
        SparkConf config = new SparkConf();
        config.set("spark.master","local");
        config.set("spark.app.name","test");  
        config.set("spark.sql.shuffle.partitions","1");
        sc = new JavaSparkContext(config);
        sqlContext = new SQLContext(sc);
    }
    
    public DataFrame toDF(String[][] data, String[] fields){
        List<String[]> asList = Arrays.asList(data);
        return toDF(asList, fields);
        
    }

    private DataFrame toDF(List<String[]> asList, String[] fields) {
        JavaRDD<Row> rowRDD = sc.parallelize(asList).map((String[] row)-> {return RowFactory.create(row);});
        List<StructField> structFields = new ArrayList<StructField>();
        for(int i=0; i < fields.length; i++){
            structFields.add(DataTypes.createStructField(fields[i], DataTypes.StringType, false)); 
        }
        StructType schema = DataTypes.createStructType(structFields);
        DataFrame df = sqlContext.createDataFrame(rowRDD, schema).toDF();
        df.show();
        return df;
    }
    
    public Customers withCustomers(){
        return new Customers();
    }
    

    public WorkOrders withWorkOrders(){
        return new WorkOrders();
    }
    public class Customers{
        List<String[]> current = new ArrayList<String[]>();
        public Customers withCurrentStatus(String id, String dataFlag, String videoFlag){
            current.add(new String[]{id, dataFlag, videoFlag});
            return this;
        }
        DataFrame df(){
            return toDF(current, getCustomersSchema());
        }
        private String[] getCustomersSchema() {
            return new String[]{"Customer_ID","Data_Flag","Video_Flag"};
        }
    }
    public class WorkOrders{
        List<String[]> workOrders = new ArrayList<String[]>();
        public WorkOrders withWorkOrder(String id, String date, String dataStatus, String videoStatus){
            workOrders.add(new String[]{id, date, dataStatus, videoStatus});
            return this;
        }
        DataFrame df(){
            return toDF(workOrders, getWorkOrdersSchema());
        }
        private String[] getWorkOrdersSchema(){
            return new String[]{"Customer_ID","Date","Data_Status","Video_Status"};
        }
    }
}
