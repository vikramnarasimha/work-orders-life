package com.sciera.workorder.life;

import org.apache.spark.sql.DataFrame;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.sciera.workorder.life.Fixture.Customers;
import com.sciera.workorder.life.Fixture.WorkOrders;

@RunWith(JUnit4.class)
public class WorkOrderLifeStatusTest {
    Fixture fixture;
    
    @Before
    public void setup(){
        fixture = new Fixture();
    }
    
    @After
    public void tearDown(){
        fixture.sc.close();
    }
    
    @Test
    public void givenWorkOrdersAndCustomers_calculateLifeStatus(){
        Customers customers = fixture.
                withCustomers().
                withCurrentStatus("1","Y","N");
            
        WorkOrders workOrders = fixture.
                withWorkOrders().
                withWorkOrder("1", "2017-01-10", "I", "D").
                withWorkOrder("1", "2017-01-11", "D", "I");
        
        
        LifeStatusChanges lifeStatusChanges = new LifeStatusChanges(customers.df(), workOrders.df());
        DataFrame calculateChanges = lifeStatusChanges.calculateChanges();
        
        new AssertDataFrame(calculateChanges).
            hasRows(2).
            containsRowWhere("Customer_ID = '%s' AND Date = '%s'", "1","2017-01-12");
        calculateChanges.show();
        
    }
    
    public class LifeStatusChanges {
        DataFrame customers;
        DataFrame workOrders;
        
        public LifeStatusChanges(DataFrame customers, DataFrame workOrders){
            this.customers = customers;
            this.workOrders = workOrders;
        }
        
        public DataFrame calculateChanges(){
            return 
                customers.join(workOrders, "Customer_ID").
                groupBy("Customer_ID","Date").count();
        }
    }
    
    
}
