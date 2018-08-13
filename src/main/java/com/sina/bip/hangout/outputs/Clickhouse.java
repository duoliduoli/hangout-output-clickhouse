package com.sina.bip.hangout.outputs;

import java.util.*;

import com.ctrip.ops.sysdev.baseplugin.BaseOutput;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Timer;
import java.util.TimerTask;


/**
 * Created by RickyHuo on 2017/09/23.
 */


public class Clickhouse extends BaseOutput {

    private static final Logger log = LogManager.getLogger(Clickhouse.class);
    private final static int BULKSIZE = 1000;

    private int bulkNum = 0;
    private int bulkSize;
    private Vector<Map> events;
    private FormatParse formatParse;

    public Clickhouse(Map config) {
        super (config);
    }

    protected void prepare() {

        // default format
        String format = "TabSeparated";
        if (this.config.containsKey("format")) {
            format = (String) this.config.get("format");
        }

        switch (format) {
            case "JSONEachRow":
                this.formatParse = new JSONEachRow(config);
                break;
            case "Values":
                this.formatParse = new Values(config);
                break;
            case "TabSeparated":
                this.formatParse = new TabSeparated(config);
                break;
            case "Native":
                this.formatParse = new Native(config);
                break;
            default:
                String msg = String.format("Unknown format <%s>", format);
                log.error(msg);
                System.exit(1);
                break;
        }

        this.formatParse.prepare();
        this.events = new Vector<>();


        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                try {
                    formatParse.bulkInsert(events);
                    log.info("Force to write to ClickHouse");
                    events.clear();
                } catch (Exception e) {
                    log.info("Failed to force to write to ClickHouse");
                    log.error(e);
                }
            }
        };
        Timer timer = new Timer();
        long intervalPeriod = 10 * 1000;
        timer.scheduleAtFixedRate(task, 10000, intervalPeriod);

        if (this.config.containsKey("bulk_size")) {
            this.bulkSize = (Integer) this.config.get("bulk_size");
        } else {
            this.bulkSize = BULKSIZE;
        }
    }

    private void eventInsert(Map event) throws Exception {
        eventInsert(event, this.bulkSize);
    }

    private void eventInsert(Map event, int eventSize) throws Exception {

        this.events.add(event);

        // 重试3次
        if (this.events.size() >= eventSize + 3) {
            this.events.clear();
            log.error("Retry 3 times failed, drop this bulk, number: " + this.bulkNum);
            this.bulkNum += 1;
        }
        if (this.events.size() >= eventSize) {

            log.info("Insert bulk start, number: " + this.bulkNum);
            this.formatParse.bulkInsert(events);
            log.info("Insert bulk end, number: " + this.bulkNum);
            this.events.clear();
            this.bulkNum += 1;
        }
    }

    protected void emit(Map event) {
        try {
            eventInsert(event);
        } catch (Exception e) {
            log.error(e);
            log.warn("insert error");
        }
    }

    public void shutdown() {
        try {
            this.formatParse.bulkInsert(this.events);
        } catch (Exception e) {
            log.info("failed to bulk events before shutdown");
            log.error(e);
        }
    }
}
