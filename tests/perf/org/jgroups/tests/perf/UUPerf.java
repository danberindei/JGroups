package org.jgroups.tests.perf;

import org.jgroups.*;
import org.jgroups.blocks.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.protocols.UNICAST;
import org.jgroups.protocols.UNICAST2;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;

import javax.management.MBeanServer;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Tests the UNICAST by invoking unicast RPCs from all the nodes to a single node.
 * Mimics state transfer in Infinispan.
 * @author Dan Berindei
 */
public class UUPerf extends ReceiverAdapter {
    private JChannel channel;
    private Address local_addr;
    private RpcDispatcher disp;
    static final String groupname="uuperf";
    private final List<Address> members=new ArrayList<Address>();


    // ============ configurable properties ==================
    private ConfigOptions config = new ConfigOptions();
    // =======================================================

    private static final Method[] METHODS=new Method[15];

    private static final short START=0;
    private static final short SET_CONFIG=1;
    private static final short GET_CONFIG=2;
    private static final short SET_PROPS=3;
    private static final short GET_PROPS=4;
    private static final short APPLY_STATE=5;

    private final AtomicInteger COUNTER=new AtomicInteger(1);


    static NumberFormat f;
    private static final int NUM_LOOPS = 100;
    private static final int TIMEOUT=120000;


    static {
        try {
            METHODS[START]=UUPerf.class.getMethod("runTest",Address.class);
            METHODS[SET_CONFIG]=UUPerf.class.getMethod("setConfig",ConfigOptions.class);
            METHODS[GET_CONFIG]=UUPerf.class.getMethod("getConfig");
            METHODS[SET_PROPS]=UUPerf.class.getMethod("setProps",String.class);
            METHODS[GET_PROPS]=UUPerf.class.getMethod("getProps");
            METHODS[APPLY_STATE]=UUPerf.class.getMethod("applyState", byte[].class);

            ClassConfigurator.add((short)12000,Results.class);
            f=NumberFormat.getNumberInstance();
            f.setGroupingUsed(false);
            f.setMinimumFractionDigits(2);
            f.setMaximumFractionDigits(2);
        }
        catch(NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }


    public void init(String props, String name) throws Throwable {
        channel=new JChannel(props);
        if(name != null)
            channel.setName(name);
        disp=new RpcDispatcher(channel,null,this,this);
        disp.setMethodLookup(new MethodLookup() {
            public Method findMethod(short id) {
                return METHODS[id];
            }
        });
        channel.connect(groupname);
        local_addr=channel.getAddress();

        try {
            MBeanServer server=Util.getMBeanServer();
            JmxConfigurator.registerChannel(channel,server,"jgroups",channel.getClusterName(),true);
        }
        catch(Throwable ex) {
            System.err.println("registering the channel in JMX failed: " + ex);
        }

        if(members.size() < 2)
            return;
        Address coord=members.get(0);
        ConfigOptions config=(ConfigOptions)disp.callRemoteMethod(coord,new MethodCall(GET_CONFIG),new RequestOptions(ResponseMode.GET_ALL,5000));
        if(config != null) {
            this.config=config;
            System.out.println("Fetched config from " + coord + ": " + config);
        }
        else
            System.err.println("failed to fetch config from " + coord);
    }

    void stop() {
        if(disp != null)
            disp.stop();
        Util.close(channel);
    }

    public void viewAccepted(View new_view) {
        System.out.println("** view: " + new_view);
        members.clear();
        members.addAll(new_view.getMembers());
    }

    // =================================== callbacks ======================================

    public Results runTest(Address dest) throws Throwable {
        System.out.println("invoking RPCs with " + config);
        final AtomicInteger num_msgs_sent=new AtomicInteger(0);

        Invoker[] invokers=new Invoker[config.num_threads];
        for(int i=0; i < invokers.length; i++)
            invokers[i]=new Invoker(dest,config.num_msgs,num_msgs_sent);

        long startNanos=System.nanoTime();
        for(Invoker invoker : invokers)
            invoker.start();

        for(Invoker invoker : invokers) {
            invoker.join();
        }

        long total_time= TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        System.out.println("done (in " + total_time + " ms)");
        return new Results(num_msgs_sent.get(),total_time);
    }


    public void setConfig(ConfigOptions config) {
        this.config=config;
        System.out.println("Updated configuration: " + config);
    }

    public ConfigOptions getConfig() {
        return config;
    }

    public void setProps(String newProps) {
        String name = channel.getName();
        int memberIndex = channel.getView().getMembers().indexOf(local_addr);
        channel.close();
        System.out.println("Stopped channel. Restarting with props " + newProps);
        try {
            Thread.sleep(memberIndex * 100);
            init(newProps, name);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }
    
    public String getProps() {
        return channel.getProperties();
    }

    public static void applyState(byte[] val) {
        System.out.println("-- applyState(): " + Util.printBytes(val.length));
    }

    // ================================= end of callbacks =====================================


    public void eventLoop() throws Throwable {
        int c;

        while(true) {
            c=Util.keyPress("[1] Send msgs [2] Print view [3] Print conns [4] Trash conn [5] Trash all conns" +
                              "\n[6] Set sender threads (" + config.num_threads + ") [7] Set num msgs (" + config.num_msgs + ") " +
                              "[8] Set msg size (" + Util.printBytes(config.msg_size) + ")" +
                              "\n[o] Toggle OOB (" + config.oob + ") [s] Toggle sync (" + config.sync + ")" +
                              "\n[f] Toggle FC (" + config.fc + ") [r] Toggle RSVP (" + config.rsvp + ")" +
                              "\n[q] Quit\n");
            ConfigOptions newConfig = new ConfigOptions(config);
            switch(c) {
                case -1:
                    break;
                case '1':
                    try {
                        startBenchmark();
                    }
                    catch(Throwable t) {
                        System.err.println(t);
                    }
                    break;
                case '2':
                    printView();
                    break;
                case '3':
                    printConnections();
                    break;
                case '4':
                    removeConnection();
                    break;
                case '5':
                    removeAllConnections();
                    break;
                case '6':
                    setSenderThreads();
                    break;
                case '7':
                    setNumMessages();
                    break;
                case '8':
                    setMessageSize();
                    break;
                case 'o':
                    newConfig.oob=!config.oob;
                    disp.callRemoteMethods(null,new MethodCall(SET_CONFIG,newConfig),RequestOptions.SYNC());
                    break;
                case 's':
                    newConfig.sync=!config.sync;
                    disp.callRemoteMethods(null,new MethodCall(SET_CONFIG,newConfig),RequestOptions.SYNC());
                    break;
                case 'f':
                    newConfig.fc=!config.fc;
                    disp.callRemoteMethods(null,new MethodCall(SET_CONFIG,newConfig),RequestOptions.SYNC());
                    break;
                case 'r':
                    newConfig.rsvp=!config.rsvp;
                    disp.callRemoteMethods(null,new MethodCall(SET_CONFIG,newConfig),RequestOptions.SYNC());
                    break;
                case 'q':
                    channel.close();
                    return;
                case '\n':
                case '\r':
                    break;
                default:
                    break;
            }
        }
    }

    private void printConnections() {
        Protocol prot=channel.getProtocolStack().findProtocol(Util.getUnicastProtocols());
        if(prot instanceof UNICAST)
            System.out.println("connections:\n" + ((UNICAST)prot).printConnections());
        else if(prot instanceof UNICAST2)
            System.out.println("connections:\n" + ((UNICAST2)prot).printConnections());
    }

    private void removeConnection() {
        Address member=getReceiver();
        if(member != null) {
            Protocol prot=channel.getProtocolStack().findProtocol(Util.getUnicastProtocols());
            if(prot instanceof UNICAST)
                ((UNICAST)prot).removeConnection(member);
            else if(prot instanceof UNICAST2)
                ((UNICAST2)prot).removeConnection(member);
        }
    }

    private void removeAllConnections() {
        Protocol prot=channel.getProtocolStack().findProtocol(Util.getUnicastProtocols());
        if(prot instanceof UNICAST)
            ((UNICAST)prot).removeAllConnections();
        else if(prot instanceof UNICAST2)
            ((UNICAST2)prot).removeAllConnections();
    }


    /**
     * Kicks off the benchmark on all cluster nodes
     */
    void startBenchmark() throws Throwable {
        // warm-up
        for (int i = 0; i < NUM_LOOPS/10; i++) {
            RspList<Results> responses = runTestLoop();
            for(Map.Entry<Address, Rsp<Results>> entry : responses.entrySet()) {
                Address mbr=entry.getKey();
                Rsp<Results> rsp=entry.getValue();
                Results result=rsp.getValue();
                // no point in continuing the warm-up if we're getting timeouts
                if (result.num_msgs < config.num_msgs)
                    break;
            }
        }

        long total_reqs=0;
        long total_time=0;
        long[] max_times = new long[NUM_LOOPS];

        for (int i = 0; i < NUM_LOOPS; i++) {
            RspList<Results> responses = runTestLoop();

            for(Map.Entry<Address, Rsp<Results>> entry : responses.entrySet()) {
                Address mbr=entry.getKey();
                Rsp<Results> rsp=entry.getValue();
                Results result=rsp.getValue();
                if (result.num_msgs < config.num_msgs) {
                    // we had an error on the remote node, stop the test
                    System.out.println("\n======================= Results: ===========================");
                    System.out.println("Timeout after " + TIMEOUT + " ms");
                    return;
                }
                total_reqs+=result.num_msgs;
                total_time+=result.time;
                max_times[i] = Math.max(max_times[i], result.time);
                System.out.println(mbr + ": " + result);
            }
        }
        Arrays.sort(max_times);
        long median_max_time = max_times[(NUM_LOOPS - 1)/2];
        long percentile_90_max_time = max_times[(NUM_LOOPS - 1)*90/100];
        long percentile_99_max_time = max_times[(NUM_LOOPS - 1)*99/100];
        long max_max_time = max_times[NUM_LOOPS - 1];

        System.out.println("\n======================= Results: ===========================");
        double total_reqs_sec=total_time!=0 ? total_reqs / (total_time / 1000.0) : 0;
        double throughput=total_reqs_sec * config.msg_size;
        double ms_per_req=total_reqs!=0 ? total_time / (double)total_reqs : 0;
        Protocol prot=channel.getProtocolStack().findProtocol(Util.getUnicastProtocols());
        System.out.println("Average of " + f.format(total_reqs_sec) + " requests/sec (" +
                                       Util.printBytes(throughput) + "/sec), " +
                                       f.format(ms_per_req) + " ms/request (prot=" + prot.getName() + ")");
        System.out.println("Max response time: " + f.format(max_max_time) + ", 99th percentile: " + f.format(percentile_99_max_time) +
                                       ", 90th percentile: " + f.format(percentile_90_max_time) + ", median: " + f.format(median_max_time));
        System.out.println("\n\n");
    }

    private RspList<Results> runTestLoop() throws Exception {
        RequestOptions options=new RequestOptions(ResponseMode.GET_ALL,0);
        options.setFlags(Message.OOB,Message.DONT_BUNDLE);
        options.setExclusionList(local_addr);
        return disp.callRemoteMethods(null, new MethodCall(START, local_addr), options);
    }


    void setSenderThreads() throws Exception {
        int new_threads=Util.readIntFromStdin("Number of sender threads: ");
        ConfigOptions newConfig = new ConfigOptions(config);
        newConfig.num_threads=new_threads;
        disp.callRemoteMethods(null,new MethodCall(SET_CONFIG,newConfig),RequestOptions.SYNC());
    }

    void setNumMessages() throws Exception {
        int new_msgs=Util.readIntFromStdin("Number of RPCs: ");
        ConfigOptions newConfig = new ConfigOptions(config);
        newConfig.num_msgs=new_msgs;
        disp.callRemoteMethods(null,new MethodCall(SET_CONFIG,newConfig),RequestOptions.SYNC());
    }

    void setMessageSize() throws Exception {
        int new_size=Util.readIntFromStdin("Message size: ");
        ConfigOptions newConfig = new ConfigOptions(config);
        newConfig.msg_size=new_size;
        disp.callRemoteMethods(null,new MethodCall(SET_CONFIG,newConfig),RequestOptions.SYNC());
    }


    void printView() {
        System.out.println("\n-- view: " + channel.getView() + '\n');
        try {
            System.in.skip(System.in.available());
        }
        catch(Exception e) {
        }
    }


    /**
     * Picks the next member in the view
     */
    private Address getReceiver() {
        try {
            List<Address> mbrs=channel.getView().getMembers();
            int index=mbrs.indexOf(local_addr);
            int new_index=index + 1 % mbrs.size();
            return mbrs.get(new_index);
        }
        catch(Exception e) {
            System.err.println("UPerf.getReceiver(): " + e);
            return null;
        }
    }

    private class Invoker extends Thread {
        private final Address dest;
        private final int num_msgs_to_send;
        private final AtomicInteger num_msgs_sent;


        public Invoker(Address dest, int num_msgs_to_send, AtomicInteger num_msgs_sent) {
            this.num_msgs_sent=num_msgs_sent;
            this.dest=dest;
            this.num_msgs_to_send=num_msgs_to_send;
            setName("Invoker-" + COUNTER.getAndIncrement());
        }


        public void run() {
            final byte[] buf=new byte[config.msg_size];
            Object[] apply_state_args={buf};
            MethodCall apply_state_call=new MethodCall(APPLY_STATE,apply_state_args);
            RequestOptions apply_state_options=new RequestOptions(config.sync? ResponseMode.GET_ALL : ResponseMode.GET_NONE,TIMEOUT,true,null);

            if(config.oob) {
                apply_state_options.setFlags(Message.OOB);
            }
            if(config.sync) {
                apply_state_options.setFlags(Message.DONT_BUNDLE);
            }
            if (!config.fc) {
                apply_state_options.setFlags(Message.NO_FC);
            }
            if (config.rsvp) {
                apply_state_options.setFlags(Message.Flag.RSVP);
            }


            while(true) {
                long i=num_msgs_sent.get();
                if(i >= num_msgs_to_send)
                    break;

                try {
                    apply_state_args[0]=buf;
                    disp.callRemoteMethod(dest,apply_state_call,apply_state_options);

                    num_msgs_sent.incrementAndGet();
                }
                catch(Throwable throwable) {
                    throwable.printStackTrace();
                    break;
                }
            }
        }

    }


    public static class Results implements Streamable {
        long num_msgs=0;
        long time=0;

        public Results() {

        }

        public Results(int num_msgs, long time) {
            this.num_msgs=num_msgs;
            this.time=time;
        }


        public void writeTo(DataOutput out) throws Exception {
            out.writeLong(num_msgs);
            out.writeLong(time);
        }

        public void readFrom(DataInput in) throws Exception {
            num_msgs=in.readLong();
            time=in.readLong();
        }

        public String toString() {
            if (time == 0)
                return "0 reqs/sec (0 APPLY_STATEs total)";

            long total_reqs=num_msgs;
            double total_reqs_per_sec=total_reqs / (time / 1000.0);

            return f.format(total_reqs_per_sec) + " reqs/sec (" + num_msgs + " APPLY_STATEs total)";
        }
    }


    public static class ConfigOptions implements Externalizable {
        private boolean sync=true, oob=true;
        private boolean fc=true, rsvp=true;
        private int num_threads=1;
        private int num_msgs=1, msg_size=4500000;

        public ConfigOptions() {
        }

        public ConfigOptions(ConfigOptions old) {
            this.oob=old.oob;
            this.sync=old.sync;
            this.fc=old.fc;
            this.rsvp=old.rsvp;
            this.num_threads=old.num_threads;
            this.num_msgs=old.num_msgs;
            this.msg_size=old.msg_size;
        }

        public ConfigOptions(boolean oob, boolean sync, boolean fc, boolean rsvp, int num_threads, int num_msgs, int msg_size) {
            this.oob=oob;
            this.sync=sync;
            this.fc=fc;
            this.rsvp=rsvp;
            this.num_threads=num_threads;
            this.num_msgs=num_msgs;
            this.msg_size=msg_size;
        }

        public String toString() {
            return "oob=" + oob + ", sync=" + sync + ", fc=" + fc + ", rsvp=" + rsvp +
              ", num_threads=" + num_threads + ", num_msgs=" + num_msgs + ", msg_size=" + msg_size;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeBoolean(oob);
            out.writeBoolean(sync);
            out.writeBoolean(fc);
            out.writeBoolean(rsvp);
            out.writeInt(num_threads);
            out.writeInt(num_msgs);
            out.writeInt(msg_size);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            oob=in.readBoolean();
            sync=in.readBoolean();
            fc=in.readBoolean();
            rsvp=in.readBoolean();
            num_threads=in.readInt();
            num_msgs=in.readInt();
            msg_size=in.readInt();
        }
    }


    public static void main(String[] args) {
        String props=null;
        String name=null;


        for(int i=0; i < args.length; i++) {
            if("-props".equals(args[i])) {
                props=args[++i];
                continue;
            }
            if("-name".equals(args[i])) {
                name=args[++i];
                continue;
            }
            help();
            return;
        }

        UUPerf test=null;
        try {
            test=new UUPerf();
            test.init(props,name);
            test.eventLoop();
        }
        catch(Throwable ex) {
            ex.printStackTrace();
            if(test != null)
                test.stop();
        }
    }

    static void help() {
        System.out.println("UPerf [-props <props>] [-name name]");
    }


}