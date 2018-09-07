// Copyright ©2018 Impinj, Inc. All rights reserved.
// You may use and modify this code under the terms of the Impinj Software Tools License & Disclaimer.
// Visit https://support.impinj.com/hc/en-us/articles/360000468370-Software-Tools-License-Disclaimer
// for full license details, or contact Impinj, Inc. at support@impinj.com for a copy of the license.

using System;
using System.Configuration;
using System.Collections;
using System.Data.SqlClient;
using System.Data;

namespace ItemSenseRDBMService
{
    class SQLServerRDBMS : RDBMSbase
    {
        protected static DataTable dtEvents = null;

        public SQLServerRDBMS(ArrayList itemEvent, ArrayList thr, ArrayList itemFile) : base(itemEvent, thr, itemFile)
        {
            #region DataTable Setup definitions

            dtEvents = new DataTable("epc_master_events");

            DataColumn epcColumn = new DataColumn();
            epcColumn.DataType = typeof(String);
            epcColumn.ColumnName = "epc_nbr";
            dtEvents.Columns.Add(epcColumn);

            DataColumn obsTmColumn = new DataColumn();
            obsTmColumn.DataType = typeof(DateTime);
            obsTmColumn.ColumnName = "last_obsv_time";
            dtEvents.Columns.Add(obsTmColumn);

            DataColumn tagIdColumn = new DataColumn();
            tagIdColumn.DataType = typeof(String);
            tagIdColumn.ColumnName = "tag_id";
            dtEvents.Columns.Add(tagIdColumn);

            DataColumn ZoneColumn = new DataColumn();
            ZoneColumn.DataType = typeof(String);
            ZoneColumn.ColumnName = "zone_name";
            dtEvents.Columns.Add(ZoneColumn);

            DataColumn FloorColumn = new DataColumn();
            FloorColumn.DataType = typeof(String);
            FloorColumn.ColumnName = "floor";
            dtEvents.Columns.Add(FloorColumn);

            DataColumn FacColumn = new DataColumn();
            FacColumn.DataType = typeof(String);
            FacColumn.ColumnName = "facility";
            dtEvents.Columns.Add(FacColumn);

            DataColumn XColumn = new DataColumn();
            XColumn.DataType = typeof(double);
            XColumn.ColumnName = "x_coord";
            dtEvents.Columns.Add(XColumn);

            DataColumn YColumn = new DataColumn();
            YColumn.DataType = typeof(double);
            YColumn.ColumnName = "y_coord";
            dtEvents.Columns.Add(YColumn);

            DataColumn UpcColumn = new DataColumn();
            UpcColumn.DataType = typeof(String);
            UpcColumn.ColumnName = "upc_nbr";
            dtEvents.Columns.Add(UpcColumn);

            DataColumn updTmColumn = new DataColumn();
            updTmColumn.DataType = typeof(DateTime);
            updTmColumn.ColumnName = "last_updt_time";
            dtEvents.Columns.Add(updTmColumn);

            #endregion
        }

        protected override void WriteRawItemEventRecordsToRDBMS()
        {
            #region debug_WriteRawItemEventRecordsToRDBMS_kpi
            DateTime blockTmSt = System.DateTime.Now;
            log.Debug("WriteRawItemEventRecordsToRDBMS started: " + blockTmSt.ToLongTimeString());
            #endregion

            #region SqlServer DDL
            //Do Not Alter - These strings are modified via the app.cfg
            //Drop and Create "updatedb_cmd"
            const string cmdText = @"IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{is_raw_item_event_hist}' AND xtype = 'U') CREATE TABLE {is_raw_item_event_hist} " +
                @"(epc_nbr VARCHAR(128) NOT NULL, tag_id VARCHAR(128),  job_id VARCHAR(128), from_zone VARCHAR(128), from_floor VARCHAR(128), to_zone VARCHAR(128), " +
                @"to_floor VARCHAR(128), from_fac VARCHAR(128), to_fac VARCHAR(128), from_x float, from_y float, to_x float, " +
                @"to_y float, obsv_time DateTime, PRIMARY KEY(epc_nbr, obsv_time)); " +
                @"DROP TABLE IF EXISTS {is_raw_item_event}; CREATE TABLE {is_raw_item_event} (epc_nbr VARCHAR(128) NOT NULL, " +
                @"tag_id VARCHAR(128) NULL, job_id VARCHAR(128), from_zone VARCHAR(128), from_floor VARCHAR(128), " +
                @"to_zone VARCHAR(128), to_floor VARCHAR(128), from_fac VARCHAR(128), to_fac VARCHAR(128), " +
                @"from_x float, from_y float, to_x float, to_y float, obsv_time DateTime);";
            string rplTxt = cmdText.Replace("{is_raw_item_event_hist}", ConfigurationManager.AppSettings["ItemSenseRawItemEventHistTableName"]);
            string cfgCmdText = rplTxt.Replace("{is_raw_item_event}", ConfigurationManager.AppSettings["ItemSenseRawItemEventTableName"]);

            //Update History "updatedb_cmd"
            const string postCmdText = @"INSERT INTO {is_raw_item_event_hist} (epc_nbr, tag_id, job_id, from_zone, from_floor, to_zone, to_floor, from_fac, to_fac, from_x, from_y, to_x, to_y, obsv_time) " +
                @"SELECT epc_nbr, tag_id, job_id, from_zone, from_floor, to_zone, to_floor, from_fac, to_fac, from_x, from_y, to_x, to_y, obsv_time FROM {is_raw_item_event}; ";
            string postRplTxt = postCmdText.Replace("{is_raw_item_event_hist}", ConfigurationManager.AppSettings["ItemSenseRawItemEventHistTableName"]);
            string postCfgCmdText = postRplTxt.Replace("{is_raw_item_event}", ConfigurationManager.AppSettings["ItemSenseRawItemEventTableName"]);

            #endregion

            string connStr = ConfigurationManager.AppSettings["DbConnectionString"];
            System.Data.DataTableReader reader = rawItemEventRecs.CreateDataReader();
            using (SqlConnection conn = new SqlConnection(connStr))
            {
                try
                {
                    conn.Open();
                    SqlCommand createdb_cmd = new SqlCommand(cfgCmdText, conn);
                    //Create history table if necessary, drop and recreate the temporary raw_item_event table
                    createdb_cmd.ExecuteNonQuery();

                    //insert into the raw_item_event table events just read
                    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn))
                    {
                        bulkCopy.DestinationTableName = ConfigurationManager.AppSettings["ItemSenseRawItemEventTableName"];
                        bulkCopy.WriteToServer(reader);
                    }

                    SqlCommand updatedb_cmd = new SqlCommand(postCfgCmdText, conn);
                    //update the raw_item_event_history table with whatever is in raw_item_event table
                    updatedb_cmd.ExecuteNonQuery();

                    log.Info("WriteRawItemEventRecordsToRDBMS rows inserted: " + rawItemEventRecs.Rows.Count.ToString());

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
                finally
                {
                    reader.Close();
                    conn.Close();
                }
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
        /// raw_item_event_hist table only timestamps for same epc_nbr in location read within the last 10 seconds in history table
        /// </summary>
        protected override void WriteSmoothedItemEventRecordsToRDBMS()
        {
            #region debug_WriteSmoothedItemEventRecordsToRDBMS_kpi
            DateTime blockTmSt = System.DateTime.Now;
            log.Debug("WriteSmoothedItemEventRecordsToRDBMS started: " + blockTmSt.ToLongTimeString());
            #endregion

            #region Postgresql DDL
            //Do Not Alter - These strings are modified via the app.cfg
            const string cmdText = @"IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{smoothed_item_event_hist}' AND xtype = 'U') CREATE TABLE {smoothed_item_event_hist} " +
                @"(epc_nbr VARCHAR(128) NOT NULL, fromX_ma float, fromY_ma float, toX_ma float, toY_ma float,  " +
                @"fromX_wma float, fromY_wma float, toX_wma float, toY_wma float, calc_time DateTime, PRIMARY KEY(epc_nbr, calc_time)); " +
                @"INSERT INTO {smoothed_item_event_hist} (epc_nbr, fromX_ma, fromY_ma, toX_ma, toY_ma, fromX_wma, fromY_wma, toX_wma, toY_wma, calc_time) " +
                @"SELECT DISTINCT z.epc_nbr, z.fromX_ma, z.fromY_ma, z.toX_ma, z.toY_ma, SUM(z.fromX_wma)/z.sum_weighted as fromX_wma, " +
                @"SUM(z.fromY_wma)/z.sum_weighted as fromY_wma, SUM(z.toX_wma)/z.sum_weighted as toX_wma, SUM(z.toY_wma)/z.sum_weighted as toY_wma, GETDATE() " +
                @"FROM (SELECT y.epc_nbr, AVG(y.from_x) as fromX_ma, AVG(y.from_y) as fromY_ma, AVG(y.to_x) as toX_ma, AVG(y.to_y) as toY_ma, " +
                @"SUM(CASE WHEN DATEDIFF(Second, x.obsv_time, y.obsv_time) <= 2 THEN 0.5 " +
                @"WHEN DATEDIFF(Second, x.obsv_time, y.obsv_time) <= 4 AND DATEDIFF(Second, x.obsv_time, y.obsv_time) > 2 THEN 0.25 " +
                @"WHEN DATEDIFF(Second, x.obsv_time, y.obsv_time) <= 6 AND DATEDIFF(Second, x.obsv_time, y.obsv_time) > 4 THEN 0.18 " +
                @"WHEN DATEDIFF(Second, x.obsv_time, y.obsv_time) <= 10 AND DATEDIFF(Second, x.obsv_time, y.obsv_time) > 6 THEN 0.07 " +
                @"END) as sum_weighted, " +
                @"SUM(CASE WHEN DATEDIFF(Second, x.obsv_time, y.obsv_time) <= 2 THEN 0.5 * y.from_x " +
                @"WHEN DATEDIFF(Second, x.obsv_time, y.obsv_time) <= 4 AND DATEDIFF(Second, x.obsv_time, y.obsv_time) > 2 THEN 0.25 * y.from_x " +
                @"WHEN DATEDIFF(Second, x.obsv_time, y.obsv_time) <= 6 AND DATEDIFF(Second, x.obsv_time, y.obsv_time) > 4 THEN 0.18 * y.from_x " +
                @"WHEN DATEDIFF(Second, x.obsv_time, y.obsv_time) <= 10 AND DATEDIFF(Second, x.obsv_time, y.obsv_time) > 6 THEN 0.07 * y.from_x " +
                @"END) as fromX_wma, " +
                @"SUM(CASE WHEN DATEDIFF(Second, x.obsv_time, y.obsv_time) <= 2 THEN 0.5 * y.from_y " +
                @"WHEN DATEDIFF(Second, x.obsv_time, y.obsv_time) <= 4 AND DATEDIFF(Second, x.obsv_time, y.obsv_time) > 2 THEN 0.25 * y.from_y " +
                @"WHEN DATEDIFF(Second, x.obsv_time, y.obsv_time) <= 6 AND DATEDIFF(Second, x.obsv_time, y.obsv_time) > 4 THEN 0.18 * y.from_y " +
                @"WHEN DATEDIFF(Second, x.obsv_time, y.obsv_time) <= 10 AND DATEDIFF(Second, x.obsv_time, y.obsv_time) > 6 THEN 0.07 * y.from_y " +
                @"END) as fromY_wma, " +
                @"SUM(CASE WHEN DATEDIFF(Second, x.obsv_time, y.obsv_time) <= 2 THEN 0.5 * y.to_x " +
                @"WHEN DATEDIFF(Second, x.obsv_time, y.obsv_time) <= 4 AND DATEDIFF(Second, x.obsv_time, y.obsv_time) > 2 THEN 0.25 * y.to_x " +
                @"WHEN DATEDIFF(Second, x.obsv_time, y.obsv_time) <= 6 AND DATEDIFF(Second, x.obsv_time, y.obsv_time) > 4 THEN 0.18 * y.to_x " +
                @"WHEN DATEDIFF(Second, x.obsv_time, y.obsv_time) <= 10 AND DATEDIFF(Second, x.obsv_time, y.obsv_time) > 6 THEN 0.07 * y.to_x " +
                @"END) as toX_wma, " +
                @"SUM(CASE WHEN DATEDIFF(Second, x.obsv_time, y.obsv_time) <= 2 THEN 0.5 * y.to_y " +
                @"WHEN DATEDIFF(Second, x.obsv_time, y.obsv_time) <= 4 AND DATEDIFF(Second, x.obsv_time, y.obsv_time) > 2 THEN 0.25 * y.to_y " +
                @"WHEN DATEDIFF(Second, x.obsv_time, y.obsv_time) <= 6 AND DATEDIFF(Second, x.obsv_time, y.obsv_time) > 4 THEN 0.18 * y.to_y " +
                @"WHEN DATEDIFF(Second, x.obsv_time, y.obsv_time) <= 10 AND DATEDIFF(Second, x.obsv_time, y.obsv_time) > 6 THEN 0.07 * y.to_y " +
                @"END) as toY_wma " +
                @"FROM {is_raw_item_event} x " +
                @"JOIN {is_raw_item_event_hist} y ON y.epc_nbr = x.epc_nbr AND DATEDIFF(Second, x.obsv_time, y.obsv_time) <= 10 AND y.obsv_time <= x.obsv_time " +
                @"GROUP BY y.epc_nbr) as z " +
                @"GROUP BY z.epc_nbr, z.fromX_ma, z.fromY_ma, z.toX_ma, z.toY_ma, z.fromX_wma, z.fromY_wma, z.toX_wma, z.toY_wma, z.sum_weighted " +
                @"ORDER BY z.epc_nbr; ";

            string rplTxt = cmdText.Replace("{smoothed_item_event_hist}", ConfigurationManager.AppSettings["SmoothedItemEventHistTableName"]);
            string cfgRplText = rplTxt.Replace("{is_raw_item_event}", ConfigurationManager.AppSettings["ItemSenseRawItemEventTableName"]);
            string cfgCmdText = cfgRplText.Replace("{is_raw_item_event_hist}", ConfigurationManager.AppSettings["ItemSenseRawItemEventHistTableName"]);


            #endregion

            string connStr = ConfigurationManager.AppSettings["DbConnectionString"];
            using (SqlConnection conn = new SqlConnection(connStr))
            {
                try
                {
                    conn.Open();
                    SqlCommand updatedb_cmd = new SqlCommand(cfgCmdText, conn);
                    updatedb_cmd.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    string errMsg = "WriteSmoothedItemEventRecordsToRDBMS Exception: " + ex.Message + "(" + ex.GetType() + ")";
                    if (null != ex.InnerException)
                        errMsg += Environment.NewLine + ex.InnerException.Message;
                    log.Error(errMsg);
                }
                finally
                {
                    conn.Close();
                }
            }

            #region debug_WriteSmoothedItemEventRecordsToRDBMS_kpi
            DateTime procTmEnd = DateTime.Now;
            TimeSpan procTmSpan = procTmEnd.Subtract(blockTmSt);
            log.Debug("WriteSmoothedItemEventRecordsToRDBMS completed(ms): " + procTmSpan.Milliseconds.ToString());
            #endregion
        }

        protected override void WriteThresholdRecordsToRDBMS()
        {
            #region debug_WriteThresholdRecordsToRDBMS_kpi
            DateTime blockTmSt = System.DateTime.Now;
            log.Debug("WriteThresholdRecordsToRDBMS started: " + blockTmSt.ToLongTimeString());
            #endregion

            #region SqlServer DDL
            //Do Not Alter - These strings are modified via the app.cfg
            //Drop and Create "updatedb_cmd"
            const string cmdText = @"IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{is_threshold_hist}' AND xtype = 'U') CREATE TABLE " +
                @"{is_threshold_hist} (epc_nbr VARCHAR(128) NOT NULL, observation_time DateTime, from_zone VARCHAR(128), to_zone VARCHAR(128)," +
                @"threshold VARCHAR(128), confidence float, job_id VARCHAR(128), dock_door VARCHAR(128), PRIMARY KEY(epc_nbr, observation_time)); " +
                @"DROP TABLE IF EXISTS {is_threshold}; CREATE TABLE {is_threshold} (epc_nbr VARCHAR(128) NOT NULL, observation_time DateTime, " +
                @"from_zone VARCHAR(128), to_zone VARCHAR(128), threshold VARCHAR(128), confidence float, job_id VARCHAR(128), dock_door VARCHAR(128));";
            string rplTxt = cmdText.Replace("{is_threshold_hist}", ConfigurationManager.AppSettings["ItemSenseThresholdHistTableName"]);
            string cfgCmdText = rplTxt.Replace("{is_threshold}", ConfigurationManager.AppSettings["ItemSenseThresholdTableName"]);

            //Update History "updatedb_cmd"
            const string postCmdText = @"INSERT INTO {is_threshold_hist} (epc_nbr, observation_time, from_zone, to_zone, threshold, confidence, job_id, dock_door) " +
                @"SELECT epc_nbr, observation_time, from_zone, to_zone, threshold, confidence, job_id, dock_door FROM {is_threshold}; ";
            string postRplTxt = postCmdText.Replace("{is_threshold_hist}", ConfigurationManager.AppSettings["ItemSenseThresholdHistTableName"]);
            string postCfgCmdText = postRplTxt.Replace("{is_threshold}", ConfigurationManager.AppSettings["ItemSenseThresholdTableName"]);


            #endregion

            string connStr = ConfigurationManager.AppSettings["DbConnectionString"];
            System.Data.DataTableReader reader = thrRecs.CreateDataReader();
            using (SqlConnection conn = new SqlConnection(connStr))
            {
                try
                {
                    conn.Open();
                    SqlCommand createdb_cmd = new SqlCommand(cfgCmdText, conn);
                    //Create history table if necessary, drop and recreate the temporary threshold table
                    createdb_cmd.ExecuteNonQuery();

                    //Bulk insert into the threshold table events just read
                    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn))
                    {
                        bulkCopy.DestinationTableName = ConfigurationManager.AppSettings["ItemSenseThresholdTableName"];
                        bulkCopy.WriteToServer(reader);
                    }

                    SqlCommand updatedb_cmd = new SqlCommand(postCfgCmdText, conn);
                    //update the threshold_history table with whatever is in threshold table
                    updatedb_cmd.ExecuteNonQuery();

                    log.Info("WriteThresholdRecordsToRDBMS rows inserted: " + thrRecs.Rows.Count.ToString());

                }
                catch (Exception ex)
                {
                    string errMsg = "WriteThresholdRecordsToRDBMS Exception: " + ex.Message + "(" + ex.GetType() + ")";
                    if (null != ex.InnerException)
                        errMsg += Environment.NewLine + ex.InnerException.Message;
                    log.Error(errMsg);
                }
                finally
                {
                    reader.Close();
                    conn.Close();
                }
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

                //Now get all last know item events and create temp table
                GetLatestEpcFromItemEventHist();

                //Merge both tables
                MergeBothTempTables();
                
                //Call upsert epc master 
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
            const string posCmdText = @"MERGE {pos} AS target USING (SELECT DISTINCT e1.upc_nbr, SUM(CASE e1.zone_name WHEN '{pos_sold}' THEN 1 ELSE 0 END) AS qty_sold, " +
                             @"SUM(CASE e1.zone_name WHEN '{pos_return}' THEN 1 ELSE 0 END) AS qty_returned, GETDATE() AS last_updt_time FROM {epc_master} e1 " +
                             @"WHERE e1.last_updt_time = (SELECT MAX(last_updt_time) FROM {epc_master} e2 where e1.epc_nbr = e2.epc_nbr) " +
                             @"AND DATEDIFF(Day, GETDATE(), e1.last_updt_time) <= {is_hist_interval} " +
                             @"AND e1.zone_name = '{pos_sold}' OR e1.zone_name = '{pos_return}' " +
                             @"GROUP BY upc_nbr) AS source (upc_nbr, qty_sold, qty_returned, last_updt_time) ON (target.upc_nbr = source.upc_nbr) " +
                             @"WHEN MATCHED THEN UPDATE SET qty_sold = source.qty_sold, qty_returned = source.qty_returned, last_updt_time = source.last_updt_time " +
                             @"WHEN NOT MATCHED THEN INSERT (upc_nbr, qty_sold, qty_returned, last_updt_time) VALUES (source.upc_nbr, source.qty_sold, source.qty_returned, " +
                             @"source.last_updt_time);";

            string replText = posCmdText.Replace("{pos}", ConfigurationManager.AppSettings["ItemSenseExtensionPosTableName"]);
            string repl2Text = replText.Replace("{pos_sold}", ConfigurationManager.AppSettings["PosQtySoldZoneName"]);
            string repl3Text = repl2Text.Replace("{pos_return}", ConfigurationManager.AppSettings["PosQtyReturnedZoneName"]);
            string repl4Text = repl3Text.Replace("{epc_master}", ConfigurationManager.AppSettings["ItemSenseExtensionEpcMasterTableName"]);
            string cfgCmdText = repl4Text.Replace("{is_hist_interval}", ConfigurationManager.AppSettings["ItemSenseEventProcessingHistoryInterval(secs)"]);

            //Shipping & Receiving Summary
            const string shpCmdText = @"MERGE {ship_rcv} As target USING (SELECT DISTINCT e1.upc_nbr, SUM(CASE e1.zone_name WHEN '{shipped}' THEN 1 ELSE 0 END) AS qty_shipped, " +
                             @"SUM(CASE e1.zone_name WHEN '{received}' THEN 1 ELSE 0 END) AS qty_received, GETDATE() as last_updt_time FROM {epc_master} e1 " +
                             @"WHERE e1.last_updt_time = (SELECT MAX(last_updt_time) FROM {epc_master} e2 where e1.epc_nbr = e2.epc_nbr) " +
                             @"AND DATEDIFF(Second, GETDATE(), e1.last_updt_time) <= {is_hist_interval} " +
                             @"AND e1.zone_name = '{shipped}' OR e1.zone_name = '{received}' " +
                             @"GROUP BY upc_nbr) AS source (upc_nbr, qty_shipped, qty_received, last_updt_time) ON (target.upc_nbr = source.upc_nbr) " +
                             @"WHEN MATCHED THEN UPDATE SET qty_shipped = source.qty_shipped, qty_received = source.qty_received, last_updt_time = source.last_updt_time " +
                             @"WHEN NOT MATCHED THEN INSERT (upc_nbr, qty_shipped, qty_received, last_updt_time) VALUES (source.upc_nbr, source.qty_shipped, source.qty_received, source.last_updt_time;";

            string rplText = shpCmdText.Replace("{ship_rcv}", ConfigurationManager.AppSettings["ItemSenseExtensionShipRecvTableName"]);
            string rpl2Text = replText.Replace("{shipped}", ConfigurationManager.AppSettings["ShipRcvQtyShippedZoneName"]);
            string rpl3Text = repl2Text.Replace("{received}", ConfigurationManager.AppSettings["ShipRcvQtyReceivedZoneName"]);
            string rpl4Text = repl3Text.Replace("{epc_master}", ConfigurationManager.AppSettings["ItemSenseExtensionEpcMasterTableName"]);
            string postCmdText = repl4Text.Replace("{is_hist_interval}", ConfigurationManager.AppSettings["ItemSenseEventProcessingHistoryInterval(secs)"]);

            #endregion
            try
            {
                string connStr = ConfigurationManager.AppSettings["DbConnectionString"];
                SqlConnection conn = new SqlConnection(connStr);
                SqlCommand insPOSdb_cmd = new SqlCommand(cfgCmdText, conn);
                conn.Open();

                // Execute the insert into pos table
                insPOSdb_cmd.ExecuteNonQuery();

                SqlCommand insSRdb_cmd = new SqlCommand(postCmdText, conn);
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
        /// Note*:  This does not truncate reference tables as well as epc_master by design
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
            const string cmdText = @"IF EXISTS(SELECT* FROM sysobjects WHERE name= '{pos}' AND xtype = 'U') " +
                                   @"DELETE FROM {pos} WHERE DATEDIFF(Day, GETDATE(), last_updt_time) > {ext_hist_interval}; " +
                                   @"IF EXISTS (SELECT * FROM sysobjects WHERE name='{ship_rcv}' AND xtype = 'U') " +
                                   @"DELETE FROM {ship_rcv} WHERE DATEDIFF(Day, GETDATE(), last_updt_time) > {ext_hist_interval}; " +
                                   @"IF EXISTS(SELECT* FROM sysobjects WHERE name= '{upc_inv_loc}' AND xtype = 'U') " +
                                   @"DELETE FROM {upc_inv_loc} WHERE DATEDIFF(Day, GETDATE(), last_updt_time) > {ext_hist_interval};";

            string replText = cmdText.Replace("{pos}", ConfigurationManager.AppSettings["ItemSenseExtensionPosTableName"]);
            string repl2Text = replText.Replace("{ship_rcv}", ConfigurationManager.AppSettings["ItemSenseExtensionShipRecvTableName"]);
            string repl3Text = repl2Text.Replace("{ext_hist_interval}", ConfigurationManager.AppSettings["ItemSenseEventProcessingHistoryInterval(secs)"]);
            string cfgCmdText = repl3Text.Replace("{upc_inv_loc}", ConfigurationManager.AppSettings["ItemSenseExtensionUpcInventoryLocationTableName"]);

            #endregion

            try
            {
                string connStr = ConfigurationManager.AppSettings["DbConnectionString"];
                SqlConnection conn = new SqlConnection(connStr);
                SqlCommand updatedb_cmd = new SqlCommand(cfgCmdText, conn);
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
            const string cmdText = @"IF EXISTS (SELECT * FROM sysobjects WHERE name='{is_raw_item_event_hist}' AND xtype = 'U') " +
                                   @"DELETE FROM {is_raw_item_event_hist} WHERE DATEDIFF(Second, GETDATE(), obsv_time) > {is_hist_interval}; " +
                                   @"IF EXISTS (SELECT * FROM sysobjects WHERE name='{is_threshold_hist}' AND xtype = 'U') " +
                                   @"DELETE FROM {is_threshold_hist} WHERE DATEDIFF(Second, GETDATE(), observation_time) > {is_hist_interval}; " +
                                   @"IF EXISTS (SELECT * FROM sysobjects WHERE name='{smoothed_item_event_hist}' AND xtype = 'U') " +
                                   @"DELETE FROM {smoothed_item_event_hist} WHERE DATEDIFF(Second, GETDATE(), calc_time) > {is_hist_interval}; ";

            string replText = cmdText.Replace("{is_raw_item_event_hist}", ConfigurationManager.AppSettings["ItemSenseRawItemEventHistTableName"]);
            string repl2Text = replText.Replace("{is_threshold_hist}", ConfigurationManager.AppSettings["ItemSenseThresholdHistTableName"]);
            string repl3Text = repl2Text.Replace("{smoothed_item_event_hist}", ConfigurationManager.AppSettings["SmoothedItemEventHistTableName"]);
            string cfgCmdText = repl3Text.Replace("{is_hist_interval}", ConfigurationManager.AppSettings["ItemSenseEventProcessingHistoryInterval(secs)"]);

            #endregion

            try
            {
                string connStr = ConfigurationManager.AppSettings["DbConnectionString"];
                SqlConnection conn = new SqlConnection(connStr);
                SqlCommand updatedb_cmd = new SqlCommand(cfgCmdText, conn);
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
            const string postText = @"MERGE {upc_inv_loc} AS target USING (SELECT DISTINCT upc_nbr, floor, zone_name, facility, count(epc_nbr), GETDATE() " +
                @"FROM {epc_master} GROUP BY upc_nbr, floor, zone_name, facility) AS source (upc_nbr, floor, zone_name, facility, " +
                @"qty, last_updt_time) ON (target.upc_nbr = source.upc_nbr AND target.floor = source.floor AND target.zone_name = source.zone_name AND target.facility = source.facility) " +
                @"WHEN MATCHED THEN UPDATE SET qty = source.qty, last_updt_time = source.last_updt_time WHEN NOT MATCHED THEN " +
                @"INSERT (upc_nbr, floor, zone_name, facility, qty, last_updt_time) VALUES (source.upc_nbr, source.floor, source.zone_name, source.facility, " +
                @"source.qty, source.last_updt_time); ";

            string repText = postText.Replace("{upc_inv_loc}", ConfigurationManager.AppSettings["ItemSenseExtensionUpcInventoryLocationTableName"]);
            string cfgCmdText = repText.Replace("{epc_master}", ConfigurationManager.AppSettings["ItemSenseExtensionEpcMasterTableName"]);
            #endregion

            try
            {
                string connStr = ConfigurationManager.AppSettings["DbConnectionString"];
                SqlConnection conn = new SqlConnection(connStr);
                SqlCommand upsertdb_cmd = new SqlCommand(cfgCmdText, conn);
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

        private static void MergeBothTempTables()
        {
            #region debug_MergeBothTempTables_kpi
            DateTime blockTmSt = System.DateTime.Now;
            log.Debug("MergeBothTempTables started: " + blockTmSt.ToLongTimeString());
            #endregion

            #region Postgresql DDL
            //Do Not Alter - These strings are modified via the app.cfg
            //Update History "updatedb_cmd"

            string updCmdText = @"IF EXISTS (SELECT * FROM sysobjects WHERE name='is_upc_tmp' AND xtype = 'U') DROP TABLE is_upc_tmp; " +
                    @"CREATE TABLE is_upc_tmp (epc_nbr varchar(128) NOT NULL,  last_obsv_time DateTime, tag_id varchar(128), " +
                    @"zone_name varchar(128), floor varchar(128), facility varchar(128), x_coord float, " +
                    @"y_coord float, upc_nbr varchar(24), last_updt_time DateTime, PRIMARY KEY(epc_nbr, last_obsv_time)); ";


            const string cmdText = @"IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{is_temp_upc}' AND xtype = 'U') CREATE TABLE " +
                    @"{is_temp_upc} (epc_nbr varchar(128) NOT NULL, last_obsv_time DateTime, tag_id varchar(128), " +
                    @"zone_name varchar(128), floor varchar(128), facility varchar(128), x_coord float, " +
                    @"y_coord float, upc_nbr varchar(24), last_updt_time DateTime, PRIMARY KEY(epc_nbr, last_obsv_time)); " +
                    @"MERGE is_upc_tmp AS target USING( " +
                    @"SELECT epc_nbr, last_obsv_time, tag_id, zone_name, floor, facility, x_coord, y_coord, upc_nbr, last_updt_time " +
                    @"FROM {is_temp_upc} t1 " +
                    @")AS source (epc_nbr, last_obsv_time, tag_id, zone_name, floor, facility, x_coord, y_coord, upc_nbr, last_updt_time) " +
                    @"ON (target.epc_nbr = source.epc_nbr) " +
                    @"WHEN MATCHED THEN UPDATE SET last_obsv_time = source.last_obsv_time, tag_id = source.tag_id, zone_name = source.zone_name, " +
                    @"floor = source.floor, facility = source.facility, x_coord = source.x_coord, y_coord = source.y_coord, " +
                    @"upc_nbr = source.upc_nbr, last_updt_time = source.last_updt_time " +
                    @"WHEN NOT MATCHED THEN INSERT (epc_nbr, last_obsv_time, tag_id, zone_name, floor, facility, x_coord, y_coord, " +
                    @"upc_nbr, last_updt_time) VALUES (source.epc_nbr, source.last_obsv_time, source.tag_id, source.zone_name, " +
                    @"source.floor, source.facility, source.x_coord, source.y_coord, source.upc_nbr, source.last_updt_time);";

            string cfgCmdText = cmdText.Replace("{is_temp_upc}", "is_upc_tmp_thresh");
            string postCmdText = cmdText.Replace("{is_temp_upc}", "is_upc_tmp_item");

            #endregion

            try
            {
                string connStr = ConfigurationManager.AppSettings["DbConnectionString"];
                SqlConnection conn = new SqlConnection(connStr);

                SqlCommand updatedb_cmd = new SqlCommand(updCmdText, conn);
                SqlCommand mergedb_cmd = new SqlCommand(cfgCmdText, conn);
                SqlCommand postdb_cmd = new SqlCommand(postCmdText, conn);

                conn.Open();

                //First drop and create
                updatedb_cmd.ExecuteNonQuery();
                // Execute the merge Threshold
                mergedb_cmd.ExecuteNonQuery();
                // Finally merge the Item Events
                postdb_cmd.ExecuteNonQuery();


                conn.Close();
            }
            catch (Exception ex)
            {
                string errMsg = "MergeBothTempTables Exception: " + ex.Message + "(" + ex.GetType() + ")";
                if (null != ex.InnerException)
                    errMsg += Environment.NewLine + ex.InnerException.Message;
                log.Error(errMsg);
            }

            #region debug_MergeBothTempTables_kpi
            DateTime procTmEnd = DateTime.Now;
            TimeSpan procTmSpan = procTmEnd.Subtract(blockTmSt);
            log.Debug("MergeBothTempTables completed(ms): " + procTmSpan.Milliseconds.ToString());
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
            const string postText = @"MERGE {epc_master} AS target USING (SELECT epc_nbr, last_obsv_time, tag_id, zone_name, floor, facility, x_coord, " +
                @"y_coord, last_updt_time, upc_nbr FROM is_upc_tmp) AS source (epc_nbr, last_obsv_time, tag_id, zone_name, floor, facility, x_coord, y_coord, " +
                @"last_updt_time, upc_nbr) ON (target.epc_nbr = source.epc_nbr AND target.last_obsv_time < source.last_obsv_time) " +
                @"WHEN MATCHED THEN UPDATE set last_obsv_time = source.last_obsv_time, " +
                @"tag_id = source.tag_id, zone_name = source.zone_name, floor = source.floor, facility = source.facility, x_coord = source.x_coord, " +
                @"y_coord = source.y_coord, last_updt_time = source.last_updt_time WHEN NOT MATCHED THEN INSERT (epc_nbr, last_obsv_time, " +
                @"tag_id, zone_name, floor, facility, x_coord, y_coord, last_updt_time, upc_nbr) VALUES (source.epc_nbr, source.last_obsv_time, source.tag_id, " +
                @"source.zone_name, source.floor, source.facility, source.x_coord, source.y_coord, source.last_updt_time, source.upc_nbr);";

            string cfgCmdText = postText.Replace("{epc_master}", ConfigurationManager.AppSettings["ItemSenseExtensionEpcMasterTableName"]);
            #endregion

            try
            {
                string connStr = ConfigurationManager.AppSettings["DbConnectionString"];
                SqlConnection conn = new SqlConnection(connStr);
                SqlCommand upsertdb_cmd = new SqlCommand(cfgCmdText, conn);
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

            string updCmdText = @"IF EXISTS (SELECT * FROM sysobjects WHERE name='is_upc_tmp' AND xtype = 'U') DROP TABLE is_upc_tmp; " +
                @"CREATE TABLE is_upc_tmp (epc_nbr varchar(128) NOT NULL,  last_obsv_time DateTime, tag_id varchar(128), " +
                @"zone_name varchar(128), floor varchar(128), facility varchar(128), x_coord float, " +
                @"y_coord float, upc_nbr varchar(24), last_updt_time DateTime,  PRIMARY KEY(epc_nbr)); ";


            #endregion

            try
            {
                string connStr = ConfigurationManager.AppSettings["DbConnectionString"];
                SqlConnection conn = new SqlConnection(connStr);
                SqlCommand selectdb_cmd = new SqlCommand(cfgCmdText, conn);
                conn.Open();

                // Execute the query and obtain a result set
                SqlDataReader dr = selectdb_cmd.ExecuteReader();

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
                                gtin.ToUpc(), DateTime.Now);
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

                //Copy the upc records into a data table so we may create a data reader for use with bulk copy
                dtEvents.Clear();
                foreach (EpcMasterRec rec in itemEventRecords)
                    dtEvents.Rows.Add(rec.Epc, rec.ObservationTime, rec.TagId, rec.ZoneName, rec.Floor, rec.Facility, rec.Xcoord, rec.Ycoord, rec.Upc, rec.LastUpdateTime);

                //Drop and create temp table
                SqlCommand update_cmd = new SqlCommand(updCmdText, conn);
                update_cmd.ExecuteNonQuery();

                DataTableReader reader = dtEvents.CreateDataReader();

                //Bulk insert into the temp upc table events just read
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn))
                {
                    bulkCopy.DestinationTableName = "is_upc_tmp";
                    bulkCopy.WriteToServer(reader);
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

            string updCmdText = @"IF EXISTS (SELECT * FROM sysobjects WHERE name='is_upc_tmp' AND xtype = 'U') DROP TABLE is_upc_tmp; " +
                @"CREATE TABLE is_upc_tmp (epc_nbr varchar(128) NOT NULL,  last_obsv_time DateTime, tag_id varchar(128), " +
                @"zone_name varchar(128), floor varchar(128), facility varchar(128), x_coord float, " +
                @"y_coord float, upc_nbr varchar(24), last_updt_time DateTime, PRIMARY KEY(epc_nbr)); ";

            #endregion

            try
            {
                string connStr = ConfigurationManager.AppSettings["DbConnectionString"];
                SqlConnection conn = new SqlConnection(connStr);
                SqlCommand selectdb_cmd = new SqlCommand(cfgCmdText, conn);
                conn.Open();

                // Execute the query and obtain a result set
                SqlDataReader dr = selectdb_cmd.ExecuteReader();

                // Pull UPC from SGTIN reads only and Output rows to array list for insertion into UPC temp table
                while (dr.Read())
                {
                    if (Convert.ToBoolean(ConfigurationManager.AppSettings["Sgtin96Encoded"]))
                    {
                        if (Sgtin96.IsValidSGTIN(dr[0].ToString()))
                        {
                            Sgtin96 gtin = Sgtin96.FromString(dr[0].ToString());
                            EpcMasterRec rec = new EpcMasterRec(dr[0].ToString(), Convert.ToDateTime(dr[1].ToString()), dr[2].ToString(),
                                string.Empty, string.Empty, string.Empty, 0, 0, gtin.ToUpc(), DateTime.Now);
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

                //Copy the upc records into a data table so we may create a data reader for use with bulk copy
                dtEvents.Clear();
                foreach (EpcMasterRec rec in thrRecords)
                    dtEvents.Rows.Add(rec.Epc, rec.ObservationTime, rec.TagId, rec.ZoneName, rec.Floor, rec.Facility, rec.Xcoord, rec.Ycoord, rec.Upc, rec.LastUpdateTime);

                //Drop and create temp table
                SqlCommand update_cmd = new SqlCommand(updCmdText, conn);
                update_cmd.ExecuteNonQuery();

                DataTableReader reader = dtEvents.CreateDataReader();

                //Bulk insert into the temp upc table events just read
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn))
                {
                    bulkCopy.DestinationTableName = "is_upc_tmp";
                    bulkCopy.WriteToServer(reader);
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
            const string cmdText = @"IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{epc_master}' AND xtype = 'U') CREATE TABLE " +
                @"{epc_master} (epc_nbr varchar(128) NOT NULL UNIQUE, last_obsv_time DateTime, tag_id varchar(128), zone_name varchar(128), " +
                @"floor varchar(128), facility varchar(128), x_coord float, y_coord float, last_updt_time DateTime,  " +
                @"upc_nbr varchar(24), PRIMARY KEY (epc_nbr)); " +
                @"IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{upc_inv_loc}' AND xtype = 'U') CREATE TABLE " +
                @"{upc_inv_loc} (upc_nbr varchar(24) NOT NULL, floor varchar(128), zone_name varchar(128), facility varchar(128), qty int, " +
                @"last_updt_time DateTime, PRIMARY KEY (upc_nbr, floor, zone_name, facility)); " +
                @"IF NOT EXISTS (SELECT * FROM sysindexes WHERE name='UK_{upc_inv_loc}_upc_floor_zone_fac') " +
                @"CREATE UNIQUE INDEX  UK_{upc_inv_loc}_upc_floor_zone_fac ON {upc_inv_loc} (upc_nbr, floor, zone_name, facility); " +
                @"IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='dept' AND xtype = 'U') CREATE TABLE " +
                @"dept (dept_nbr int, dept_desc varchar(128), zone_name varchar(128), floor varchar(128), facility varchar(128), PRIMARY KEY (dept_nbr)); " +
                @"IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='item' AND xtype = 'U') CREATE TABLE " +
                @"item (upc_nbr varchar(24), dept_nbr int, retail_price float, item_cost float, item_nbr int, avg_rate_of_sale float, " +
                @"item_desc varchar(128), mfg_name varchar(128), shelf_qty int, on_hand int, PRIMARY KEY (upc_nbr)); " +
                @"IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{pos}' AND xtype = 'U') CREATE TABLE " +
                @"{pos} (upc_nbr varchar(24) NOT NULL, qty_sold int, qty_returned int, last_updt_time DateTime, PRIMARY KEY (upc_nbr, last_updt_time)); " +
                @"IF NOT EXISTS (SELECT * FROM sysindexes WHERE name='UK_{pos}_upc_lastupdt' ) " +
                @"CREATE UNIQUE INDEX UK_{pos}_upc_lastupdt ON {pos} (upc_nbr, qty_sold, qty_returned); " +
                @"IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{ship_rcv}' AND xtype = 'U') CREATE TABLE " +
                @"{ship_rcv} (upc_nbr varchar(24) NOT NULL, qty_shipped int, qty_received int, last_updt_time DateTime, PRIMARY KEY (upc_nbr, last_updt_time)); " +
                @"IF NOT EXISTS (SELECT * FROM sysindexes WHERE name='UK_{ship_rcv}_upc_ship_rcvd') " +
                @"CREATE UNIQUE INDEX UK_{ship_rcv}_upc_ship_rcvd ON {ship_rcv} (upc_nbr, qty_shipped, qty_received); ";

            string rplTxt = cmdText.Replace("{epc_master}", ConfigurationManager.AppSettings["ItemSenseExtensionEpcMasterTableName"]);
            string rpl2Text = rplTxt.Replace("{upc_inv_loc}", ConfigurationManager.AppSettings["ItemSenseExtensionUpcInventoryLocationTableName"]);
            string rpl3Text = rpl2Text.Replace("{pos}", ConfigurationManager.AppSettings["ItemSenseExtensionPosTableName"]);
            string cfgCmdText = rpl3Text.Replace("{ship_rcv}", ConfigurationManager.AppSettings["ItemSenseExtensionShipRecvTableName"]);

            #endregion

            try
            {
                string connStr = ConfigurationManager.AppSettings["DbConnectionString"];
                SqlConnection conn = new SqlConnection(connStr);
                SqlCommand createdb_cmd = new SqlCommand(cfgCmdText, conn);
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
