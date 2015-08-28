package org.wso2.carbon.event.processor.common.storm.benchmarks.emailprocessor;

import com.uebercomputing.mailrecord.MailRecord;
import org.wso2.carbon.event.processor.common.storm.benchmarks.emailprocessor.util.Constants;

import java.util.concurrent.LinkedBlockingQueue;


/**
 * Created by miyurud on 4/9/15.
 */
public class EmailProcessor {
    private static LinkedBlockingQueue<Object> eventBufferList = null;
    private static String inputfilePath = "/home/miyurud/Projects/CEPStormPerf/EmailProcessorBenchmark/datasets/Enron/Avro/enron.avro";
    public static void main(String[] args){
        eventBufferList = new LinkedBlockingQueue<Object>(Constants.EVENT_BUFFER_SIZE);

        FilterOperator filterOperator = new FilterOperator();
        MetricsOperator metricsOperator = new MetricsOperator();
        ModifyOperator modifyOperator = new ModifyOperator();

        DataLoderThread dataLoderThread = new DataLoderThread(inputfilePath, eventBufferList);
        dataLoderThread.start();

        while(true){
            try {
                MailRecord obj = (MailRecord)eventBufferList.take();

                obj = filterOperator.process(obj);

                if(obj != null) {
                    obj = modifyOperator.process(obj);
                    obj = metricsOperator.process(obj);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
