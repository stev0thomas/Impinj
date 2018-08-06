﻿// Copyright ©2018 Impinj, Inc. All rights reserved.
// You may use and modify this code under the terms of the Impinj Software Tools License & Disclaimer.
// Visit https://support.impinj.com/hc/en-us/articles/360000468370-Software-Tools-License-Disclaimer
// for full license details, or contact Impinj, Inc. at support@impinj.com for a copy of the license.

using System;
using System.Configuration;
using Npgsql;
using System.Collections;
using ImpinjItemSenseRDBMService;

namespace ItemSenseRDBMService
{
    class PostgreSqlRDBMS : RDBMSbase
    {

        public PostgreSqlRDBMS(ArrayList itemEvent, ArrayList thr, ArrayList itemFile) : base(itemEvent, thr, itemFile)
        {
        }

        /// <summary>
        /// This is a callback on a new Thread to Insert raw ItemSense ItemEvent records into RDBMS
        /// </summary>
        protected override void WriteRawItemEventRecordsToRDBMS()
        {
            #region debug_WriteRawItemEventRecordsToRDBMS_kpi
            DateTime blockTmSt = System.DateTime.Now;
            log.Debug("WriteRawItemEventRecordsToRDBMS started: " + blockTmSt.ToLongTimeString());
            #endregion

            #region Postgresql DDL
            //Do Not Alter - These strings are modified via the app.cfg
            //Drop and Create "createdb_cmd"
            const string cmdText = @"CREATE TABLE IF NOT EXISTS {is_raw_item_event_hist} (epc_nbr character varying(128) NOT NULL, tag_id character varying(128),  job_id character varying(128), " +
                @"from_zone character varying(128), from_floor character varying(128), to_zone character varying(128), to_floor character varying(128), from_fac character varying(128), " +
                @"to_fac character varying(128), from_x double precision, from_y double precision, to_x double precision, to_y double precision, obsv_time timestamp, PRIMARY KEY(epc_nbr, obsv_time) " +
                @")WITH(OIDS= FALSE); " +
                @"DROP TABLE IF EXISTS {is_raw_item_event}; CREATE TABLE {is_raw_item_event} (epc_nbr character varying(128) NOT NULL, " +
                @"tag_id character varying(128) NULL, job_id character varying(128), from_zone character varying(128), from_floor character varying(128), " +
                @"to_zone character varying(128), to_floor character varying(128), from_fac character varying(128), to_fac character varying(128), " +
                @"from_x double precision, from_y double precision, to_x double precision, to_y double precision, obsv_time timestamp)WITH(OIDS= FALSE);";
            string rplTxt = cmdText.Replace("{is_raw_item_event_hist}", ConfigurationManager.AppSettings["ItemSenseRawItemEventHistTableName"]);
            string cfgCmdText = rplTxt.Replace("{is_raw_item_event}", ConfigurationManager.AppSettings["ItemSenseRawItemEventTableName"]);

            //Bulk Insert
            string tmpTxt = "COPY {is_raw_item_event}(epc_nbr, tag_id, job_id, from_zone, from_floor, to_zone, to_floor, from_fac, to_fac, from_x, from_y, to_x, to_y, obsv_time) FROM STDIN WITH DELIMITER ',' CSV";
            string impText = tmpTxt.Replace("{is_raw_item_event}", ConfigurationManager.AppSettings["ItemSenseRawItemEventTableName"]);

            //Update History "updatedb_cmd"
            const string postText = @"INSERT INTO {is_raw_item_event_hist} (epc_nbr, tag_id, job_id, from_zone, from_floor, to_zone, to_floor, from_fac, to_fac, from_x, from_y, to_x, to_y, obsv_time) " +
                @"SELECT epc_nbr, tag_id, job_id, from_zone, from_floor, to_zone, to_floor, from_fac, to_fac, from_x, from_y, to_x, to_y, obsv_time FROM {is_raw_item_event}; ";
            string postRplTxt = postText.Replace("{is_raw_item_event_hist}", ConfigurationManager.AppSettings["ItemSenseRawItemEventHistTableName"]);
            string postCfgCmdText = postRplTxt.Replace("{is_raw_item_event}", ConfigurationManager.AppSettings["ItemSenseRawItemEventTableName"]);

            #endregion

            try
            {
                string connStr = ConfigurationManager.AppSettings["DbConnectionString"];
                NpgsqlConnection conn = new NpgsqlConnection(connStr);
                NpgsqlCommand createdb_cmd = new NpgsqlCommand(cfgCmdText, conn);
                conn.Open();
                //Create history table if necessary, drop and recreate the temporary raw_item_event table
                createdb_cmd.ExecuteNonQuery();

                //Bulk insert into the raw_item_event table events just read
                foreach (ItemEventRec rec in itemEventRecords)
                {
                    using (var writer = conn.BeginTextImport(impText))
                    {
                        string dbg = rec.ItemEventRecToCsvString();
                        writer.WriteLine(rec.ItemEventRecToCsvString());
                    }
                }

                //update the raw_item_event_history table with whatever is in raw_item_event table
                NpgsqlCommand updatedb_cmd = new NpgsqlCommand(postCfgCmdText, conn);
                updatedb_cmd.ExecuteNonQuery();
                conn.Close();

                log.Info("WriteRawItemEventRecordsToRDBMS rows inserted: " + itemEventRecords.Count.ToString());

                if (Convert.ToBoolean(ConfigurationManager.AppSettings["SmoothDataXY"]))
                    WriteSmoothedItemEventRecordsToRDBMS();
            }
            catch (Exception ex)
            {
                string errMsg = "WriteRawItemEventRecordsToRDBMS Exception: " + ex.Message + "(" + ex.GetType() + ")";
                if (null != ex.InnerException)
                    errMsg += Environment.NewLine + ex.InnerException.Message;
                log.Error(errMsg);
            }

            #region debug_WriteRawItemEventRecordsToRDBMS_kpi
            DateTime procTmEnd = DateTime.Now;
            TimeSpan procTmSpan = procTmEnd.Subtract(blockTmSt);
            log.Debug("WriteRawItemEventRecordsToRDBMS completed(ms): " + procTmSpan.Milliseconds.ToString());
            #endregion
        }

        /// <summary>
        /// This function relies on the database to do the moving average and weighted moving average calculations on X and Y coords
        /// of the reads.  The algorithm compares what was inserted into the raw_item_event table and compares with the 
        /// raw_item_event_hist table only timestamps for same epc_nbr in item_event read within the last 10 seconds in history table
        /// </summary>
        protected override void WriteSmoothedItemEventRecordsToRDBMS()
        {
            #region debug_WriteSmoothedItemEventRecordsToRDBMS_kpi
            DateTime blockTmSt = System.DateTime.Now;
            log.Debug("WriteSmoothedItemEventRecordsToRDBMS started: " + blockTmSt.ToLongTimeString());
            #endregion

            #region Postgresql DDL
            //Do Not Alter - These strings are modified via the app.cfg
            const string cmdText = @"CREATE TABLE IF NOT EXISTS {smoothed_item_event_hist} (epc_nbr character varying(128) NOT NULL,  " +
                @"fromX_ma double precision, fromY_ma double precision, toX_ma double precision, toY_ma double precision,  " +
                @"fromX_wma double precision, fromY_wma double precision, toX_wma double precision, toY_wma double precision, calc_time timestamp, PRIMARY KEY(epc_nbr, calc_time) " +
                @")WITH(OIDS= FALSE); " +
                @"INSERT INTO {smoothed_item_event_hist} (epc_nbr, fromX_ma, fromY_ma, toX_ma, toY_ma, fromX_wma, fromY_wma, toX_wma, toY_wma, calc_time) " +
                @"SELECT DISTINCT z.epc_nbr, z.fromX_ma, z.fromY_ma, z.toX_ma, z.toY_ma, SUM(z.fromX_wma)/z.sum_weighted as fromX_wma, " +
                @"SUM(z.fromY_wma)/z.sum_weighted as fromY_wma, SUM(z.toX_wma)/z.sum_weighted as toX_wma, SUM(z.toY_wma)/z.sum_weighted as toY_wma, current_timestamp " +
                @"FROM (SELECT y.epc_nbr, AVG(y.from_x) as fromX_ma, AVG(y.from_y) as fromY_ma, AVG(y.to_x) as toX_ma, AVG(y.to_y) as toY_ma, " +
                @"SUM(CASE 	WHEN x.obsv_time - y.obsv_time <= interval '2 seconds' THEN 0.5 " +
                @"WHEN x.obsv_time - y.obsv_time <= interval '4 seconds' AND x.obsv_time - y.obsv_time > interval '2 seconds' THEN 0.25 " +
                @"WHEN x.obsv_time - y.obsv_time <= interval '6 seconds' AND x.obsv_time - y.obsv_time > interval '4 seconds' THEN 0.18 " +
                @"WHEN x.obsv_time - y.obsv_time <= interval '10 seconds' AND x.obsv_time - y.obsv_time > interval '6 seconds' THEN 0.07 " +
                @"END) as sum_weighted, " +
                @"SUM(CASE WHEN x.obsv_time - y.obsv_time <= interval '2 seconds' THEN 0.5 * y.from_x " +
                @"WHEN x.obsv_time - y.obsv_time <= interval '4 seconds' AND x.obsv_time - y.obsv_time > interval '2 seconds' THEN 0.25 * y.from_x " +
                @"WHEN x.obsv_time - y.obsv_time <= interval '6 seconds' AND x.obsv_time - y.obsv_time > interval '4 seconds' THEN 0.18 * y.from_x " +
                @"WHEN x.obsv_time - y.obsv_time <= interval '10 seconds'  AND x.obsv_time - y.obsv_time > interval '6 seconds' THEN 0.07 * y.from_x " +
                @"END) as fromX_wma, " +
                @"SUM(CASE WHEN x.obsv_time - y.obsv_time <= interval '2 seconds' THEN 0.5 * y.from_y " +
                @"WHEN x.obsv_time - y.obsv_time <= interval '4 seconds' AND x.obsv_time - y.obsv_time > interval '2 seconds' THEN 0.25 * y.from_y " +
                @"WHEN x.obsv_time - y.obsv_time <= interval '6 seconds' AND x.obsv_time - y.obsv_time > interval '4 seconds' THEN 0.18 * y.from_y " +
                @"WHEN x.obsv_time - y.obsv_time <= interval '10 seconds'  AND x.obsv_time - y.obsv_time > interval '6 seconds' THEN 0.07 * y.from_y " +
                @"END) as fromY_wma, " +
                @"SUM(CASE WHEN x.obsv_time - y.obsv_time <= interval '2 seconds' THEN 0.5 * y.to_x " +
                @"WHEN x.obsv_time - y.obsv_time <= interval '4 seconds' AND x.obsv_time - y.obsv_time > interval '2 seconds' THEN 0.25 * y.to_x " +
                @"WHEN x.obsv_time - y.obsv_time <= interval '6 seconds' AND x.obsv_time - y.obsv_time > interval '4 seconds' THEN 0.18 * y.to_x " +
                @"WHEN x.obsv_time - y.obsv_time <= interval '10 seconds'  AND x.obsv_time - y.obsv_time > interval '6 seconds' THEN 0.07 * y.to_x " +
                @"END) as toX_wma, " +
                @"SUM(CASE WHEN x.obsv_time - y.obsv_time <= interval '2 seconds' THEN 0.5 * y.to_y " +
                @"WHEN x.obsv_time - y.obsv_time <= interval '4 seconds' AND x.obsv_time - y.obsv_time > interval '2 seconds' THEN 0.25 * y.to_y " +
                @"WHEN x.obsv_time - y.obsv_time <= interval '6 seconds' AND x.obsv_time - y.obsv_time > interval '4 seconds' THEN 0.18 * y.to_y " +
                @"WHEN x.obsv_time - y.obsv_time <= interval '10 seconds'  AND x.obsv_time - y.obsv_time > interval '6 seconds' THEN 0.07 * y.to_y " +
                @"END) as toY_wma " +
                @"FROM {is_raw_item_event} x " +
                @"JOIN {is_raw_item_event_hist} y ON y.epc_nbr = x.epc_nbr AND y.obsv_time >= x.obsv_time - interval '10 seconds' AND y.obsv_time <= x.obsv_time " +
                @"GROUP BY y.epc_nbr) as z " +
                @"GROUP BY z.epc_nbr, z.fromX_ma, z.fromY_ma, z.toX_ma, z.toY_ma, z.fromX_wma, z.fromY_wma, z.toX_wma, z.toY_wma, z.sum_weighted " +
                @"ORDER BY z.epc_nbr; ";

            string rplTxt = cmdText.Replace("{smoothed_item_event_hist}", ConfigurationManager.AppSettings["SmoothedItemEventHistTableName"]);
            string cfgRplText = rplTxt.Replace("{is_raw_item_event}", ConfigurationManager.AppSettings["ItemSenseRawItemEventTableName"]);
            string cfgCmdText = cfgRplText.Replace("{is_raw_item_event_hist}", ConfigurationManager.AppSettings["ItemSenseRawItemEventHistTableName"]);
            #endregion

            try
            {
                string connStr = ConfigurationManager.AppSettings["DbConnectionString"];
                NpgsqlConnection conn = new NpgsqlConnection(connStr);
                NpgsqlCommand updatedb_cmd = new NpgsqlCommand(cfgCmdText, conn);
                conn.Open();
                updatedb_cmd.ExecuteNonQuery();
                conn.Close();
            }
            catch (Exception ex)
            {
                string errMsg = "WriteSmoothedItemEventRecordsToRDBMS Exception: " + ex.Message + "(" + ex.GetType() + ")";
                if (null != ex.InnerException)
                    errMsg += Environment.NewLine + ex.InnerException.Message;
                log.Error(errMsg);
            }

            #region debug_WriteSmoothedItemEventRecordsToRDBMS_kpi
            DateTime procTmEnd = DateTime.Now;
            TimeSpan procTmSpan = procTmEnd.Subtract(blockTmSt);
            log.Debug("WriteSmoothedItemEventRecordsToRDBMS completed(ms): " + procTmSpan.Milliseconds.ToString());
            #endregion
        }

  
        /// <summary>
        /// This is a callback on a new Thread to Insert ItemSense Threshold records into RDBMS
        /// </summary>
        protected override void WriteThresholdRecordsToRDBMS()
        {
            #region debug_WriteThresholdRecordsToRDBMS_kpi
            DateTime blockTmSt = System.DateTime.Now;
            log.Debug("WriteThresholdRecordsToRDBMS started: " + blockTmSt.ToLongTimeString());
            #endregion

            #region Postgresql DDL
            //Do Not Alter - These strings are modified via the app.cfg
            //Drop and Create "createdb_cmd"
            const string cmdText = @"CREATE TABLE IF NOT EXISTS {is_threshold_hist} (epc_nbr character varying(128) NOT NULL, observation_time timestamptz, from_zone character varying(128), " +
                @"to_zone character varying(128), threshold character varying(128), confidence float(1), job_id character varying(128), dock_door character varying(128), PRIMARY KEY (epc_nbr, observation_time) " +
                @")WITH(OIDS= FALSE); " +
                @"DROP TABLE IF EXISTS {is_threshold}; CREATE TABLE {is_threshold} (epc_nbr character varying(128) NOT NULL, observation_time timestamptz, " +
                @"from_zone character varying(128), to_zone character varying(128), threshold character varying(128), confidence float(1), job_id character varying(128), " +
                @"dock_door character varying(128), PRIMARY KEY (epc_nbr, observation_time))WITH(OIDS= FALSE);";
            string rplTxt = cmdText.Replace("{is_threshold_hist}", ConfigurationManager.AppSettings["ItemSenseThresholdHistTableName"]);
            string cfgCmdText = rplTxt.Replace("{is_threshold}", ConfigurationManager.AppSettings["ItemSenseThresholdTableName"]);

            //Bulk Insert 
            string tmpTxt = "COPY {is_threshold}(epc_nbr, observation_time, from_zone, to_zone, threshold, confidence, job_id, dock_door) FROM STDIN WITH DELIMITER ',' CSV";
            string impText = tmpTxt.Replace("{is_threshold}", ConfigurationManager.AppSettings["ItemSenseThresholdTableName"]);

            //Update History "updatedb_cmd
            const string postText = @"INSERT INTO {is_threshold_hist} (epc_nbr, observation_time, from_zone, to_zone, threshold, confidence, job_id, dock_door) " +
                @"SELECT epc_nbr, observation_time, from_zone, to_zone, threshold, confidence, job_id, dock_door FROM {is_threshold}; ";
            string postRplTxt = postText.Replace("{is_threshold_hist}", ConfigurationManager.AppSettings["ItemSenseThresholdHistTableName"]);
            string postCfgCmdText = postRplTxt.Replace("{is_threshold}", ConfigurationManager.AppSettings["ItemSenseThresholdTableName"]);
            #endregion

            try
            {
                string connStr = ConfigurationManager.AppSettings["DbConnectionString"];
                NpgsqlConnection conn = new NpgsqlConnection(connStr);
                NpgsqlCommand createdb_cmd = new NpgsqlCommand(cfgCmdText, conn);
                conn.Open();
                //Create history table if necessary, drop and recreate the temporary threshold table
                createdb_cmd.ExecuteNonQuery();

                //Bulk insert into the threshold table events just read
                foreach (ThresholdRec rec in thrRecords)
                {
                    using (var writer = conn.BeginTextImport(impText))
                    {
                        string dbg = rec.ThresholdRecToCsvString();
                        writer.WriteLine(rec.ThresholdRecToCsvString());
                    }
                }

                //update the threshold history table with whatever is in threshold table
                NpgsqlCommand updatedb_cmd = new NpgsqlCommand(postCfgCmdText, conn);
                updatedb_cmd.ExecuteNonQuery();
                conn.Close();

                log.Info("WriteThresholdRecordsToRDBMS rows inserted: " + thrRecords.Count.ToString());
            }
            catch (Exception ex)
            {
                string errMsg = "WriteThresholdRecordsToRDBMS Exception: " + ex.Message + "(" + ex.GetType() + ")";
                if (null != ex.InnerException)
                    errMsg += Environment.NewLine + ex.InnerException.Message;
                log.Error(errMsg);
            }

            #region debug_WriteThresholdRecordsToRDBMS_kpi
            DateTime procTmEnd = DateTime.Now;
            TimeSpan procTmSpan = procTmEnd.Subtract(blockTmSt);
            log.Debug("WriteThresholdRecordsToRDBMS completed(ms): " + procTmSpan.Milliseconds.ToString());
            #endregion

        }

        public void ProcessItemSenseMessages()
        {
            #region debug_processItemSense_msg_kpi
            DateTime blockTmSt = System.DateTime.Now;
            log.Debug("ProcessItemSenseMessages started: " + blockTmSt.ToLongTimeString());
            #endregion

            try
            {
                //First create extension tables if they don't exist
                CreateItemSenseRdbmsExtensionTables();

                //Next get last known threshold events and create temp table
                GetLatestEpcFromThresholdHist();
                //Call upsert epc master 
                UpsertEpcMasterFromTempTable();

                //Now get all last know item events and create temp table
                GetLatestEpcFromItemEventHist();
                //Call upsert epc master again as temp table will now have ItemEventHistory data
                UpsertEpcMasterFromTempTable();

                //Now upsert the count for each upc at each location
                UpsertUpcInventoryLocation();

                //Truncate the ItemSense Tables to size configured in app.config
                TruncateItemSenseHist();

                //Insert into summary tables based on business logic tied to zones assigned to threshold data
                InsertThresholdSummaryData();

                //Finally Truncate the Extension Tables to size configured
                TruncateExtensionTables();

            }
            catch (Exception ex)
            {
                string errMsg = "ProcessItemSenseMessages Exception: " + ex.Message + "(" + ex.GetType() + ")";
                if (null != ex.InnerException)
                    errMsg += Environment.NewLine + ex.InnerException.Message;
                log.Error(errMsg);
            }

            #region debug_processItemSense_msg_kpi
            DateTime procTmEnd = DateTime.Now;
            TimeSpan procTmSpan = procTmEnd.Subtract(blockTmSt);
            log.Debug("ProcessItemSenseMessages completed(ms): " + procTmSpan.Milliseconds.ToString());
            #endregion
        }

        private void InsertThresholdSummaryData()
        {
            #region debug_InsertThresholdSummaryData_kpi
            DateTime blockTmSt = System.DateTime.Now;
            log.Debug("InsertThresholdSummaryData started: " + blockTmSt.ToLongTimeString());
            #endregion

            #region Postgresql DDL
            //Do Not Alter - These strings are modified via the app.cfg
            //Point of Sale Summary
            const string posCmdText = @"INSERT INTO {pos} (upc_nbr, qty_sold, qty_returned, last_updt_time) " +
                             @"SELECT DISTINCT e1.upc_nbr, SUM(CASE e1.zone_name WHEN '{pos_sold}' THEN 1 ELSE 0 END) AS qty_sold, " +
                             @"SUM(CASE e1.zone_name WHEN '{pos_return}' THEN 1 ELSE 0 END) AS qty_returned, current_timestamp as last_updt_time FROM {epc_master} e1 " +
                             @"WHERE e1.last_updt_time = (SELECT MAX(last_updt_time) FROM {epc_master} e2 where e1.epc_nbr = e2.epc_nbr) " +
                             @"AND current_timestamp - e1.last_updt_time <= interval '{is_hist_interval} seconds' " +
                             @"AND e1.zone_name = '{pos_sold}' OR e1.zone_name = '{pos_return}' " +
                             @"GROUP BY upc_nbr " +
                             @"ON CONFLICT (upc_nbr, qty_sold, qty_returned) DO UPDATE SET qty_sold = excluded.qty_sold, qty_returned = excluded.qty_returned, last_updt_time = excluded.last_updt_time; ";

            string replText = posCmdText.Replace("{pos}", ConfigurationManager.AppSettings["ItemSenseExtensionPosTableName"]);
            string repl2Text = replText.Replace("{pos_sold}", ConfigurationManager.AppSettings["PosQtySoldZoneName"]);
            string repl3Text = repl2Text.Replace("{pos_return}", ConfigurationManager.AppSettings["PosQtyReturnedZoneName"]);
            string repl4Text = repl3Text.Replace("{epc_master}", ConfigurationManager.AppSettings["ItemSenseExtensionEpcMasterTableName"]);
            string cfgCmdText = repl4Text.Replace("{is_hist_interval}", ConfigurationManager.AppSettings["ItemSenseEventProcessingHistoryInterval(secs)"]);

            //Shipping & Receiving Summary
            const string shpCmdText = @"INSERT INTO {ship_rcv} (upc_nbr, qty_shipped, qty_received, last_updt_time) " +
                             @"SELECT DISTINCT e1.upc_nbr, SUM(CASE e1.zone_name WHEN '{shipped}' THEN 1 ELSE 0 END) AS qty_shipped, " +
                             @"SUM(CASE e1.zone_name WHEN '{received}' THEN 1 ELSE 0 END) AS qty_received, current_timestamp as last_updt_time FROM {epc_master} e1 " +
                             @"WHERE e1.last_updt_time = (SELECT MAX(last_updt_time) FROM {epc_master} e2 where e1.epc_nbr = e2.epc_nbr) " +
                             @"AND current_timestamp - e1.last_updt_time <= interval '{is_hist_interval} seconds' " +
                             @"AND e1.zone_name = '{shipped}' OR e1.zone_name = '{received}' " +
                             @"GROUP BY upc_nbr " +
                             @"ON CONFLICT (upc_nbr, qty_shipped, qty_received) DO UPDATE SET qty_shipped = excluded.qty_shipped, " +
                             @"qty_received = excluded.qty_receiveed, last_updt_time = excluded.last_updt_time; ";

            string rplText = shpCmdText.Replace("{ship_rcv}", ConfigurationManager.AppSettings["ItemSenseExtensionShipRecvTableName"]);
            string rpl2Text = replText.Replace("{shipped}", ConfigurationManager.AppSettings["ShipRcvQtyShippedZoneName"]);
            string rpl3Text = repl2Text.Replace("{received}", ConfigurationManager.AppSettings["ShipRcvQtyReceivedZoneName"]);
            string rpl4Text = repl3Text.Replace("{epc_master}", ConfigurationManager.AppSettings["ItemSenseExtensionEpcMasterTableName"]);
            string postCmdText = repl4Text.Replace("{is_hist_interval}", ConfigurationManager.AppSettings["ItemSenseEventProcessingHistoryInterval(secs)"]);

            #endregion
            try
            {
                string connStr = ConfigurationManager.AppSettings["DbConnectionString"];
                NpgsqlConnection conn = new NpgsqlConnection(connStr);
                NpgsqlCommand insPOSdb_cmd = new NpgsqlCommand(cfgCmdText, conn);
                conn.Open();

                // Execute the insert into pos table
                insPOSdb_cmd.ExecuteNonQuery();

                NpgsqlCommand insSRdb_cmd = new NpgsqlCommand(postCmdText, conn);
                // Execute the insert into ship_rcv table
                insSRdb_cmd.ExecuteNonQuery();

                conn.Close();
            }
            catch (Exception ex)
            {
                string errMsg = "InsertThresholdSummaryData Exception: " + ex.Message + "(" + ex.GetType() + ")";
                if (null != ex.InnerException)
                    errMsg += Environment.NewLine + ex.InnerException.Message;
                log.Error(errMsg);
            }

            #region debug_InsertThresholdSummaryData_kpi
            DateTime procTmEnd = DateTime.Now;
            TimeSpan procTmSpan = procTmEnd.Subtract(blockTmSt);
            log.Debug("InsertThresholdSummaryData completed(ms): " + procTmSpan.Milliseconds.ToString());
            #endregion
        }

        /// <summary>
        /// Truncates all summary table information in the Extension 
        /// Note*:  This does not truncate reference tables as well as epc_master table by design
        /// </summary>
        private void TruncateExtensionTables()
        {
            #region debug_TruncateExtensionTables_kpi
            DateTime blockTmSt = System.DateTime.Now;
            log.Debug("TruncateExtensionTables started: " + blockTmSt.ToLongTimeString());
            #endregion

            #region Postgresql DDL
            //Do Not Alter - These strings are modified via the app.cfg
            //Update History "updatedb_cmd"
            const string cmdText = @"IF EXISTS (SELECT * FROM pg_table WHERE tablename='{pos}') " +
                                   @"DELETE FROM {pos} WHERE current_timestamp - last_updt_time > interval '{ext_hist_interval} days'; " +
                                   @"IF EXISTS (SELECT * FROM pg_table WHERE tablename='{ship_rcv}') " +
                                   @"DELETE FROM {ship_rcv} WHERE current_timestamp - last_updt_time > interval '{ext_hist_interval} days'; " +
                                   @"IF EXISTS (SELECT * FROM pg_table WHERE tablename='{upc_inv_loc}') " +
                                   @"DELETE FROM {upc_inv_loc} WHERE current_timestamp - last_updt_time > interval '{ext_hist_interval} days';";

            string replText = cmdText.Replace("{pos}", ConfigurationManager.AppSettings["ItemSenseExtensionPosTableName"]);
            string repl2Text = replText.Replace("{ship_rcv}", ConfigurationManager.AppSettings["ItemSenseExtensionShipRecvTableName"]);
            string repl3Text = repl2Text.Replace("{ext_hist_interval}", ConfigurationManager.AppSettings["ItemSenseEventProcessingHistoryInterval(secs)"]);
            string cfgCmdText = repl3Text.Replace("{upc_inv_loc}", ConfigurationManager.AppSettings["ItemSenseExtensionUpcInventoryLocationTableName"]);

            #endregion

            try
            {
                string connStr = ConfigurationManager.AppSettings["DbConnectionString"];
                NpgsqlConnection conn = new NpgsqlConnection(connStr);
                NpgsqlCommand updatedb_cmd = new NpgsqlCommand(cfgCmdText, conn);
                conn.Open();

                // Execute the truncation
                updatedb_cmd.ExecuteNonQuery();
                conn.Close();
            }
            catch (Exception ex)
            {
                string errMsg = "TruncateExtensionTables Exception: " + ex.Message + "(" + ex.GetType() + ")";
                if (null != ex.InnerException)
                    errMsg += Environment.NewLine + ex.InnerException.Message;
                log.Error(errMsg);
            }

            #region debug_TruncateExtensionTables_kpi
            DateTime procTmEnd = DateTime.Now;
            TimeSpan procTmSpan = procTmEnd.Subtract(blockTmSt);
            log.Debug("TruncateExtensionTables completed(ms): " + procTmSpan.Milliseconds.ToString());
            #endregion
        }

        private static void TruncateItemSenseHist()
        {
            #region debug_TruncateItemEventHist_kpi
            DateTime blockTmSt = System.DateTime.Now;
            log.Debug("TruncateItemSenseHist started: " + blockTmSt.ToLongTimeString());
            #endregion

            #region Postgresql DDL
            //Do Not Alter - These strings are modified via the app.cfg
            //Update History "updatedb_cmd"
            const string cmdText = @"IF EXISTS (SELECT * FROM pg_table WHERE tablename='{is_raw_item_event_hist}') " +
                                   @"DELETE FROM {is_raw_item_event_hist} WHERE current_timestamp - obsv_time > interval '{is_hist_interval} seconds'; " +
                                   @"IF EXISTS (SELECT * FROM pg_table WHERE tablename='{is_threshold_hist}') " +
                                   @"DELETE FROM {is_threshold_hist} WHERE current_timestamp - observation_time > interval '{is_hist_interval} seconds'; " +
                                   @"IF EXISTS (SELECT * FROM pg_table WHERE tablename='{smoothed_item_event_hist}') " +
                                   @"DELETE FROM {smoothed_item_event_hist} WHERE current_timestamp - calc_time > interval '{is_hist_interval} seconds'; ";

            string replText = cmdText.Replace("{is_raw_item_event_hist}", ConfigurationManager.AppSettings["ItemSenseRawItemEventHistTableName"]);
            string repl2Text = replText.Replace("{is_threshold_hist}", ConfigurationManager.AppSettings["ItemSenseThresholdHistTableName"]);
            string repl3Text = repl2Text.Replace("{smoothed_item_event_hist}", ConfigurationManager.AppSettings["SmoothedItemEventHistTableName"]);
            string cfgCmdText = repl3Text.Replace("{is_hist_interval}", ConfigurationManager.AppSettings["ItemSenseEventProcessingHistoryInterval(secs)"]);

            #endregion

            try
            {
                string connStr = ConfigurationManager.AppSettings["DbConnectionString"];
                NpgsqlConnection conn = new NpgsqlConnection(connStr);
                NpgsqlCommand updatedb_cmd = new NpgsqlCommand(cfgCmdText, conn);
                conn.Open();

                // Execute the truncation
                updatedb_cmd.ExecuteNonQuery();
                conn.Close();
            }
            catch (Exception ex)
            {
                string errMsg = "TruncateItemSenseHist Exception: " + ex.Message + "(" + ex.GetType() + ")";
                if (null != ex.InnerException)
                    errMsg += Environment.NewLine + ex.InnerException.Message;
                log.Error(errMsg);
            }

            #region debug_TruncateItemEventHist_kpi
            DateTime procTmEnd = DateTime.Now;
            TimeSpan procTmSpan = procTmEnd.Subtract(blockTmSt);
            log.Debug("TruncateItemSenseHist completed(ms): " + procTmSpan.Milliseconds.ToString());
            #endregion
        }

        private static void UpsertUpcInventoryLocation()
        {
            #region debug_UpsertUpcInventoryLocation_kpi
            DateTime blockTmSt = System.DateTime.Now;
            log.Debug("UpsertUpcInventoryLocation started: " + blockTmSt.ToLongTimeString());
            #endregion

            #region Postgresql DDL
            //Do Not Alter - These strings are modified via the app.cfg
            //Update History "upsertdb_cmd"
            const string postText = @"INSERT INTO {upc_inv_loc} (upc_nbr, floor, zone_name, facility, qty, last_updt_time) " +
                @"SELECT DISTINCT upc_nbr, floor, zone_name, facility, count(epc_nbr) as qty, current_timestamp FROM {epc_master} " +
                @"GROUP BY upc_nbr, floor, zone_name, facility " +
                @"ON CONFLICT (upc_nbr, floor, zone_name, facility) DO UPDATE SET qty = excluded.qty, last_updt_time = excluded.last_updt_time; ";
            string repText = postText.Replace("{upc_inv_loc}", ConfigurationManager.AppSettings["ItemSenseExtensionUpcInventoryLocationTableName"]);
            string cfgCmdText = repText.Replace("{epc_master}", ConfigurationManager.AppSettings["ItemSenseExtensionEpcMasterTableName"]);
            #endregion

            try
            {
                string connStr = ConfigurationManager.AppSettings["DbConnectionString"];
                NpgsqlConnection conn = new NpgsqlConnection(connStr);
                NpgsqlCommand upsertdb_cmd = new NpgsqlCommand(cfgCmdText, conn);
                conn.Open();

                // Execute the insert / update to upc_inv_loc
                upsertdb_cmd.ExecuteNonQuery();
                conn.Close();
            }
            catch (Exception ex)
            {
                string errMsg = "UpsertUpcInventoryLocation Exception: " + ex.Message + "(" + ex.GetType() + ")";
                if (null != ex.InnerException)
                    errMsg += Environment.NewLine + ex.InnerException.Message;
                log.Error(errMsg);
            }

            #region debug_processItemSense_msg_kpi
            DateTime procTmEnd = DateTime.Now;
            TimeSpan procTmSpan = procTmEnd.Subtract(blockTmSt);
            log.Debug("UpsertUpcInventoryLocation completed(ms): " + procTmSpan.Milliseconds.ToString());
            #endregion
        }

        private static void UpsertEpcMasterFromTempTable()
        {
            #region debug_UpsertEpcMasterFromTempTable_kpi
            DateTime blockTmSt = System.DateTime.Now;
            log.Debug("UpsertEpcMasterFromTempTable started: " + blockTmSt.ToLongTimeString());
            #endregion

            #region Postgresql DDL
            //Do Not Alter - These strings are modified via the app.cfg
            //Update Epc Master History "upsertdb_cmd"
            const string postText = @"INSERT INTO {epc_master} (epc_nbr, last_obsv_time, tag_id, zone_name, floor, facility, x_coord, y_coord, last_updt_time, upc_nbr) " +
                @"SELECT epc_nbr, last_obsv_time, tag_id, zone_name, floor, facility, x_coord, y_coord, last_updt_time, upc_nbr FROM is_upc_tmp " +
                @"ON CONFLICT (epc_nbr) DO UPDATE SET last_obsv_time = excluded.last_obsv_time, zone_name = excluded.zone_name, floor = excluded.floor, " +
                @"facility = excluded.facility, x_coord = excluded.x_coord, y_coord = excluded.y_coord, last_updt_time = excluded.last_updt_time; ";

            string cfgCmdText = postText.Replace("{epc_master}", ConfigurationManager.AppSettings["ItemSenseExtensionEpcMasterTableName"]);
            #endregion

            try
            {
                string connStr = ConfigurationManager.AppSettings["DbConnectionString"];
                NpgsqlConnection conn = new NpgsqlConnection(connStr);
                NpgsqlCommand upsertdb_cmd = new NpgsqlCommand(cfgCmdText, conn);
                conn.Open();

                // Execute the insert / update to epc_master
                upsertdb_cmd.ExecuteNonQuery();
                conn.Close();
            }
            catch (Exception ex)
            {
                string errMsg = "UpsertEpcMasterFromTempTable Exception: " + ex.Message + "(" + ex.GetType() + ")";
                if (null != ex.InnerException)
                    errMsg += Environment.NewLine + ex.InnerException.Message;
                log.Error(errMsg);
            }

            #region debug_processItemSense_msg_kpi
            DateTime procTmEnd = DateTime.Now;
            TimeSpan procTmSpan = procTmEnd.Subtract(blockTmSt);
            log.Debug("UpsertEpcMasterFromTempTable completed(ms): " + procTmSpan.Milliseconds.ToString());
            #endregion
        }

        private static void GetLatestEpcFromItemEventHist()
        {
            #region debug_GetLatestEpcFromItemEventHist_kpi
            DateTime blockTmSt = System.DateTime.Now;
            log.Debug("GetLatestEpcFromItemEventHist started: " + blockTmSt.ToLongTimeString());
            #endregion


            #region Postgresql DDL
            //Do Not Alter - These strings are modified via the app.cfg
            const string cmdText = @"SELECT i1.epc_nbr, i1.obsv_time, i1.tag_id, i1.to_zone, i1.to_floor, i1.to_fac, " +
                                    @"i1.to_x, i1.to_y FROM {is_raw_item_event_hist} i1 " +
                                    @"WHERE i1.obsv_time = (SELECT MAX(obsv_time) FROM {is_raw_item_event_hist} i2 WHERE i1.epc_nbr = i2.epc_nbr) " +
                                    @"GROUP BY epc_nbr, obsv_time, tag_id, to_zone, to_floor, to_fac, to_x, to_y; ";

            string cfgCmdText = cmdText.Replace("{is_raw_item_event_hist}", ConfigurationManager.AppSettings["ItemSenseRawItemEventHistTableName"]);

            string updCmdText = @"DROP TABLE IF EXISTS is_upc_tmp; " +
                @"CREATE TABLE is_upc_tmp (epc_nbr character varying(128) NOT NULL,  last_obsv_time timestamptz, tag_id character varying(128), " +
                @"zone_name character varying(128), floor character varying(128), facility character varying(128), x_coord double precision, " +
                @"y_coord double precision, upc_nbr character varying(24), last_updt_time timestamptz, PRIMARY KEY(epc_nbr))WITH(OIDS= FALSE); ";

            //Bulk Insert
            string impText = @"COPY is_upc_tmp(epc_nbr, last_obsv_time, tag_id, zone_name, floor, facility, x_coord, y_coord, upc_nbr, last_updt_time) FROM STDIN WITH DELIMITER ',' CSV";

            #endregion

            try
            {
                string connStr = ConfigurationManager.AppSettings["DbConnectionString"];
                NpgsqlConnection conn = new NpgsqlConnection(connStr);
                NpgsqlCommand selectdb_cmd = new NpgsqlCommand(cfgCmdText, conn);
                conn.Open();

                // Execute the query and obtain a result set
                NpgsqlDataReader dr = selectdb_cmd.ExecuteReader();

                // Pull UPC from SGTIN reads only and Output rows to array list for insertion into UPC temp table
                while (dr.Read())
                {
                    if (Convert.ToBoolean(ConfigurationManager.AppSettings["Sgtin96Encoded"]))
                    {
                        if (Sgtin96.IsValidSGTIN(dr[0].ToString()))
                        {
                            Sgtin96 gtin = Sgtin96.FromString(dr[0].ToString());
                            EpcMasterRec rec = new EpcMasterRec(dr[0].ToString(), Convert.ToDateTime(dr[1].ToString()), dr[2].ToString(),
                                dr[3].ToString(), dr[4].ToString(), dr[5].ToString(), Convert.ToDouble(dr[6]), Convert.ToDouble(dr[7]),
                                gtin.ToUpc(), Convert.ToDateTime(dr[1].ToString()));
                            itemEventRecords.Add(rec);
                        }
                        else
                        {
                            log.Warn("Invalid SGTIN96 detected: " + dr[0].ToString());
                        }
                    }
                    else
                    {
                        //Do proprietary encoding filter to upc_nbr
                        EpcMasterRec rec = new EpcMasterRec(dr[0].ToString(), Convert.ToDateTime(dr[1].ToString()), dr[2].ToString(),
                            dr[3].ToString(), dr[4].ToString(), dr[5].ToString(), Convert.ToDouble(dr[6]), Convert.ToDouble(dr[7]),
                            GetCustomUpc(dr[0].ToString()), Convert.ToDateTime(dr[1].ToString()));
                        itemEventRecords.Add(rec);
                    }
                }

                dr.Close();

                //Drop and create temp table
                NpgsqlCommand update_cmd = new NpgsqlCommand(updCmdText, conn);
                update_cmd.ExecuteNonQuery();

                //Bulk insert into the temp upc table events just read
                foreach (EpcMasterRec rec in itemEventRecords)
                {
                    using (var writer = conn.BeginTextImport(impText))
                    {
                        string dbg = rec.EpcMasterEventRecToCsvString();
                        writer.WriteLine(rec.EpcMasterEventRecToCsvString());
                    }
                }
                conn.Close();
                log.Debug("GetLatestEpcFromItemEventHist rows inserted to temp table: " + itemEventRecords.Count.ToString());
            }
            catch (Exception ex)
            {
                string errMsg = "GetLatestEpcFromItemEventHist Exception: " + ex.Message + "(" + ex.GetType() + ")";
                if (null != ex.InnerException)
                    errMsg += Environment.NewLine + ex.InnerException.Message;
                log.Error(errMsg);
            }


            #region debug_GetLatestEpcFromItemEventHist_kpi
            DateTime procTmEnd = DateTime.Now;
            TimeSpan procTmSpan = procTmEnd.Subtract(blockTmSt);
            log.Debug("DoItemEventRecordsETLM completed(ms): " + procTmSpan.Milliseconds.ToString());
            #endregion

        }

        private void GetLatestEpcFromThresholdHist()
        {
            #region debug_GetLatestEpcFromThresholdHist_kpi
            DateTime blockTmSt = System.DateTime.Now;
            log.Debug("GetLatestEpcFromThresholdHist started: " + blockTmSt.ToLongTimeString());
            #endregion


            #region Postgresql DDL
            //Do Not Alter - These strings are modified via the app.cfg
            const string cmdText = @"SELECT i1.epc_nbr, i1.observation_time, i1.to_zone FROM {is_threshold_hist} i1 " +
                                   @"WHERE i1.observation_time = (SELECT MAX(observation_time) FROM {is_threshold_hist} i2 WHERE i1.epc_nbr = i2.epc_nbr) " +
                                   @"GROUP BY epc_nbr, observation_time, to_zone; ";

            string cfgCmdText = cmdText.Replace("{is_threshold_hist}", ConfigurationManager.AppSettings["ItemSenseThresholdHistTableName"]);

            string updCmdText = @"DROP TABLE IF EXISTS is_upc_tmp; " +
                @"CREATE TABLE is_upc_tmp (epc_nbr character varying(128) NOT NULL,  last_obsv_time timestamptz, tag_id character varying(128), " +
                @"zone_name character varying(128), floor character varying(128), facility character varying(128), x_coord double precision, " +
                @"y_coord double precision, upc_nbr character varying(24), last_updt_time timestamptz, PRIMARY KEY(epc_nbr))WITH(OIDS= FALSE); ";

            //Bulk Insert
            string impText = @"COPY is_upc_tmp(epc_nbr, last_obsv_time, tag_id, zone_name, floor, facility, x_coord, y_coord, upc_nbr, last_updt_time) FROM STDIN WITH DELIMITER ',' CSV";

            #endregion

            try
            {
                string connStr = ConfigurationManager.AppSettings["DbConnectionString"];
                NpgsqlConnection conn = new NpgsqlConnection(connStr);
                NpgsqlCommand selectdb_cmd = new NpgsqlCommand(cfgCmdText, conn);
                conn.Open();

                // Execute the query and obtain a result set
                NpgsqlDataReader dr = selectdb_cmd.ExecuteReader();

                // Pull UPC from SGTIN reads only and Output rows to array list for insertion into UPC temp table
                while (dr.Read())
                {
                    if (Convert.ToBoolean(ConfigurationManager.AppSettings["Sgtin96Encoded"]))
                    {
                        if (Sgtin96.IsValidSGTIN(dr[0].ToString()))
                        {
                            Sgtin96 gtin = Sgtin96.FromString(dr[0].ToString());
                            EpcMasterRec rec = new EpcMasterRec(dr[0].ToString(), Convert.ToDateTime(dr[1].ToString()), dr[2].ToString(),
                                string.Empty, string.Empty, string.Empty, 0, 0, gtin.ToUpc(), Convert.ToDateTime(dr[1].ToString()));
                            thrRecords.Add(rec);
                        }
                        else
                        {
                            log.Warn("Invalid SGTIN96 detected: " + dr[0].ToString());
                        }
                    }
                    else
                    {
                        //Do proprietary encoding filter to upc_nbr
                        EpcMasterRec rec = new EpcMasterRec(dr[0].ToString(), Convert.ToDateTime(dr[1].ToString()), dr[2].ToString(),
                            dr[3].ToString(), dr[4].ToString(), dr[5].ToString(), Convert.ToDouble(dr[6]), Convert.ToDouble(dr[7]),
                            GetCustomUpc(dr[0].ToString()), Convert.ToDateTime(dr[1].ToString()));
                        itemEventRecords.Add(rec);
                    }
                }

                dr.Close();

                //Drop and create temp table
                NpgsqlCommand update_cmd = new NpgsqlCommand(updCmdText, conn);
                update_cmd.ExecuteNonQuery();

                //Bulk insert into the temp upc table events just read
                foreach (EpcMasterRec rec in thrRecords)
                {
                    using (var writer = conn.BeginTextImport(impText))
                    {
                        string dbg = rec.EpcMasterEventRecToCsvString();
                        writer.WriteLine(rec.EpcMasterEventRecToCsvString());
                    }
                }
                conn.Close();
                log.Debug("GetLatestEpcFromThresholdHist rows inserted to temp table: " + itemEventRecords.Count.ToString());
            }
            catch (Exception ex)
            {
                string errMsg = "GetLatestEpcFromThresholdHist Exception: " + ex.Message + "(" + ex.GetType() + ")";
                if (null != ex.InnerException)
                    errMsg += Environment.NewLine + ex.InnerException.Message;
                log.Error(errMsg);
            }


            #region debug_GetLatestEpcFromThresholdHist_kpi
            DateTime procTmEnd = DateTime.Now;
            TimeSpan procTmSpan = procTmEnd.Subtract(blockTmSt);
            log.Debug("GetLatestEpcFromThresholdHist completed(ms): " + procTmSpan.Milliseconds.ToString());
            #endregion
        }

        private void CreateItemSenseRdbmsExtensionTables()
        {
            #region debug_CreateItemSenseRdbmsExtensionTables_kpi
            DateTime blockTmSt = System.DateTime.Now;
            log.Debug("CreateItemSenseRdbmsExtensionTables started: " + blockTmSt.ToLongTimeString());
            #endregion

            #region Postgresql DDL
            //Create "createdb_cmd"
            const string cmdText = @"CREATE TABLE IF NOT EXISTS {epc_master} (epc_nbr character varying(128) NOT NULL UNIQUE, last_obsv_time timestamptz DEFAULT current_timestamp, " +
                @"tag_id character varying(128) DEFAULT 'ABSENT', zone_name character varying(128) NOT NULL DEFAULT 'ABSENT', floor character varying(128) DEFAULT 'ABSENT', " +
                @"facility character varying(128) NOT NULL DEFAULT 'ABSENT', x_coord float(1) DEFAULT 0, y_coord float(1) DEFAULT 0, " +
                @"last_updt_time timestamptz DEFAULT current_timestamp, upc_nbr character varying(24) NOT NULL DEFAULT 'ABSENT', " +
                @"PRIMARY KEY (epc_nbr, last_obsv_time))WITH(OIDS= FALSE); " +
                @"CREATE TABLE IF NOT EXISTS {upc_inv_loc} (upc_nbr character varying(24) NOT NULL, floor character varying(128) NOT NULL DEFAULT 'ABSENT', " +
                @"zone_name character varying(128) NOT NULL DEFAULT 'ABSENT', facility character varying(128) NOT NULL DEFAULT 'ABSENT', qty bigint, " +
                @"last_updt_time timestamptz DEFAULT current_timestamp, " +
                @"PRIMARY KEY (upc_nbr, floor, zone_name, facility))WITH(OIDS= FALSE); " +
                @"CREATE UNIQUE INDEX IF NOT EXISTS UK_{upc_inv_loc}_upc_floor_zone_fac ON {upc_inv_loc} (upc_nbr, floor, zone_name, facility); " +
                @"CREATE TABLE IF NOT EXISTS dept (dept_nbr int, dept_desc character varying(128), zone_name character varying(128), floor character varying(128), " +
                @"facility character varying(128), PRIMARY KEY (dept_nbr))WITH(OIDS= FALSE); " +
                @"CREATE TABLE IF NOT EXISTS item (upc_nbr character varying(24), dept_nbr int, retail_price float(2), item_cost float(2), item_nbr bigint, avg_rate_of_sale float(1), " +
                @"item_desc character varying(128), mfg_name character varying(128), shelf_qty int, on_hand int, PRIMARY KEY (upc_nbr))WITH(OIDS= FALSE); " +
                @"CREATE TABLE IF NOT EXISTS {pos} (upc_nbr character varying(24) NOT NULL, qty_sold bigint, qty_returned bigint, last_updt_time timestamptz DEFAULT current_timestamp, " +
                @"PRIMARY KEY (upc_nbr, qty_sold, qty_returned))WITH(OIDS= FALSE); " +
                @"CREATE UNIQUE INDEX IF NOT EXISTS UK_{pos}_upc_lastupdt ON {pos} (upc_nbr, qty_sold, qty_returned); " +
                @"CREATE TABLE IF NOT EXISTS {ship_rcv} (upc_nbr character varying(24) NOT NULL, qty_shipped bigint, qty_received bigint, last_updt_time timestamptz DEFAULT current_timestamp, " +
                @"PRIMARY KEY (upc_nbr, qty_shipped, qty_received))WITH(OIDS= FALSE); " +
                @"CREATE UNIQUE INDEX IF NOT EXISTS UK_{ship_rcv}_upc_ship_rcvd ON {ship_rcv} (upc_nbr, qty_shipped, qty_received); ";


            string rplTxt = cmdText.Replace("{epc_master}", ConfigurationManager.AppSettings["ItemSenseExtensionEpcMasterTableName"]);
            string rpl2Text = rplTxt.Replace("{upc_inv_loc}", ConfigurationManager.AppSettings["ItemSenseExtensionUpcInventoryLocationTableName"]);
            string rpl3Text = rpl2Text.Replace("{pos}", ConfigurationManager.AppSettings["ItemSenseExtensionPosTableName"]);
            string cfgCmdText = rpl3Text.Replace("{ship_rcv}", ConfigurationManager.AppSettings["ItemSenseExtensionShipRecvTableName"]);

            #endregion

            try
            {
                string connStr = ConfigurationManager.AppSettings["DbConnectionString"];
                NpgsqlConnection conn = new NpgsqlConnection(connStr);
                NpgsqlCommand createdb_cmd = new NpgsqlCommand(cfgCmdText, conn);
                conn.Open();
                //Create history table if necessary, drop and recreate the temporary threshold table
                createdb_cmd.ExecuteNonQuery();
                conn.Close();
            }
            catch (Exception ex)
            {
                string errMsg = "CreateItemSenseRdbmsExtensionTables Exception: " + ex.Message + "(" + ex.GetType() + ")";
                if (null != ex.InnerException)
                    errMsg += Environment.NewLine + ex.InnerException.Message;
                log.Error(errMsg);
            }

            #region debug_CreateItemSenseRdbmsExtensionTables_kpi
            DateTime procTmEnd = DateTime.Now;
            TimeSpan procTmSpan = procTmEnd.Subtract(blockTmSt);
            log.Debug("CreateItemSenseRdbmsExtensionTables completed(ms): " + procTmSpan.Milliseconds.ToString());
            #endregion
        }
    }
}