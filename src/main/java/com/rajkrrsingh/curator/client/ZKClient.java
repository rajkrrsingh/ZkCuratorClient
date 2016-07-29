package com.rajkrrsingh.curator.client;

import backtype.storm.Config;
import backtype.storm.utils.Utils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by rasingh on 7/29/16.
 */
public class ZKClient {

    private CuratorFramework _curator;
    private String _zkPath;
    private String _topic;

    public ZKClient(String zkStr, String zkPath, Map conf){
        try {
            _curator = CuratorFrameworkFactory.newClient(
                    zkStr,
                    Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT)),
                    15000,
                    new RetryNTimes(
                            Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_TIMES)),
                            Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL))));
            _curator.start();
        }
        catch (Exception ex) {

        }
    }

    public static void main(String[] args) {
        if(args.length<2){
            System.out.println("usage : ZKClient zkConnectionStr zkPath");
            System.out.println("e.g. arguments to pass localhost:2181 /brokers/ids/0");
            System.exit(1);
        }
        Map conf = new HashMap();
        conf.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 1000);
        conf.put(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT, 1000);
        conf.put(Config.STORM_ZOOKEEPER_RETRY_TIMES, 4);
        conf.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL, 5);

        ZKClient zkClient = new ZKClient(args[0],args[1],conf);
        zkClient.printBrokerInfoPath(args[1]);
    }
    public void printBrokerInfoPath(String path) {

        try {
            byte[] brokerData = _curator.getData().forPath(path);
            System.out.println("brokerData :"+new String(brokerData,"UTF-8"));
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
