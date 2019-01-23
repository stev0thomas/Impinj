// Copyright ©2018 Impinj, Inc. All rights reserved.
// You may use and modify this code under the terms of the Impinj Software Tools License & Disclaimer.
// Visit https://support.impinj.com/hc/en-us/articles/360000468370-Software-Tools-License-Disclaimer
// for full license details, or contact Impinj, Inc. at support@impinj.com for a copy of the license.

//#define DEBUG
#undef DEBUG

using System;
using System.IO;
using System.Threading;
using System.Data;
using System.Configuration;
using System.Diagnostics;

namespace ItemSenseRDBMService
{
    class RDBMSbase
    {
        protected static System.Collections.ArrayList itemFileRecords = null;
        protected static System.Collections.ArrayList itemEventRecords = null;
        protected static System.Collections.ArrayList thrRecords = null;
        protected static System.Collections.ArrayList mastEventRecords = null;

        protected static DataTable rawItemEventRecs = null;
        protected static DataTable thrRecs = null;

        protected static System.Diagnostics.EventLog iLog;
        protected static int eventId = 1;


        enum RecordType
        {
            ItemEvent,
            Threshold
        };

        public RDBMSbase(System.Collections.ArrayList mastEvent, System.Collections.ArrayList itemEvent, System.Collections.ArrayList thr, System.Collections.ArrayList itemFile)
        {
            iLog = new System.Diagnostics.EventLog();
            iLog.Source = "ItemSense RDBMS Service";
            iLog.Log = "Impinj IS RDBMS Service";

            itemEventRecords = new System.Collections.ArrayList();
            thrRecords = new System.Collections.ArrayList();
            itemFileRecords = new System.Collections.ArrayList();
            mastEventRecords = new System.Collections.ArrayList();

            //Make a deep copy of the array lists so that the global ones can be cleared
            if (itemEvent.Count > 0)
            { 
                foreach (ItemEventRec lrec in itemEvent)
                    itemEventRecords.Add(lrec);
            }
            if (thr.Count > 0)
            {
                foreach (ThresholdRec trec in thr)
                    thrRecords.Add(trec);
            }
            if (itemFile.Count > 0)
            {
                foreach (ItemFileRec Irec in itemFile)
                    itemFileRecords.Add(Irec);
            }
        }

        public void ProcessAMQPmessages()
        {
            #if (DEBUG)
            #region debug_processAMQP_msg_kpi
                        DateTime blockTmSt = System.DateTime.Now;
                        iLog.WriteEntry("ProcessAMQP started: " + blockTmSt.ToLongTimeString(), EventLogEntryType.Information, eventId); eventId++;
            #endregion
            #endif

            try
            {

                if (itemEventRecords.Count > 0)
                {
                    //Insert the Raw Item Sense records
                    if (WriteRawItemEventRecordsToTable())
                    {
                        if (Convert.ToBoolean(ConfigurationManager.AppSettings["WriteRawToRDBMS"]))
                        {
                            //kick off thread to do bulk copy
                            Thread locThread = new Thread(new ThreadStart(WriteRawItemEventRecordsToRDBMS));
                            locThread.Start();
                        }

                        if (Convert.ToBoolean(ConfigurationManager.AppSettings["WriteRawToCSV"]))
                        {
                            //kick off thread to do CSV write
                            Thread csvThread = new Thread(new ThreadStart(WriteRawItemEventRecordsToCSV));
                            csvThread.Start();
                        }

                        if (Convert.ToBoolean(ConfigurationManager.AppSettings["WriteRawToXML"]))
                        {
                            //kick off thread to do XML write
                            Thread xmlThread = new Thread(new ThreadStart(WriteRawItemEventRecordsToXML));
                            xmlThread.Start();
                        }
                    }
                    else
                        iLog.WriteEntry("Failed to write raw location recs to in-memory tables.", EventLogEntryType.Error, eventId); eventId++;

                }

                if (thrRecords.Count > 0)
                {
                    //Insert the threshold Item Sense records
                    if (WriteThresholdRecordsToTable())
                    {
                        if (Convert.ToBoolean(ConfigurationManager.AppSettings["WriteRawToRDBMS"]))
                        {
                            //kick off thread to do bulk copy
                            Thread thrThread = new Thread(new ThreadStart(WriteThresholdRecordsToRDBMS));
                            thrThread.Start();
                        }

                        if (Convert.ToBoolean(ConfigurationManager.AppSettings["WriteRawToCSV"]))
                        {
                            //kick off thread to do CSV write
                            Thread csvThread = new Thread(new ThreadStart(WriteThresholdRecordsToCSV));
                            csvThread.Start();
                        }

                        if (Convert.ToBoolean(ConfigurationManager.AppSettings["WriteRawToXML"]))
                        {
                            //kick off thread to do XML write
                            Thread xmlThread = new Thread(new ThreadStart(WriteThresholdRecordsToXML));
                            xmlThread.Start();
                        }
                    }
                    else
                        iLog.WriteEntry("Failed to write threshold recs to in-memory tables.", EventLogEntryType.Error, eventId); eventId++;
                }
            }
            catch (Exception ex)
            {
                string errMsg = "ProcessAMQPmessages Exception: " + ex.Message + "(" + ex.GetType() + ")";
                if (null != ex.InnerException)
                    errMsg += Environment.NewLine + ex.InnerException.Message;
                iLog.WriteEntry(errMsg, EventLogEntryType.Error, eventId); eventId++;
            }

            #if (DEBUG)
            #region debug_processAMQP_msg_kpi
                        DateTime procTmEnd = DateTime.Now;
                        TimeSpan procTmSpan = procTmEnd.Subtract(blockTmSt);
                        iLog.WriteEntry("AMQP Processing completed(ms): " + procTmSpan.Milliseconds.ToString(), EventLogEntryType.Information, eventId); eventId++;
            #endregion
            #endif

        }

        /// <summary>
        /// Implemented in the inherited class
        /// </summary>
        protected virtual void WriteRawItemEventRecordsToRDBMS() { }

        /// <summary>
        /// Implemented in the inherited class
        /// </summary>
        protected virtual void WriteSmoothedItemEventRecordsToRDBMS() { }

        /// <summary>
        /// Implemented in the inherited class
        /// </summary>
        protected virtual void WriteThresholdRecordsToRDBMS() { }


        private void WriteThresholdRecordsToXML()
        {
            #if (DEBUG)
            #region debug_WriteThresholdRecordsToXML_kpi
                        DateTime blockTmSt = System.DateTime.Now;
                        iLog.WriteEntry("WriteThresholdRecordsToXML started: " + blockTmSt.ToLongTimeString(), EventLogEntryType.Information, eventId); eventId++;
            #endregion
            #endif

            try
            {
                DataTable prev = new DataTable("is_thresh");
                prev.Columns.Add("epc", typeof(string));
                prev.Columns.Add("observationTime", typeof(DateTime));
                prev.Columns.Add("fromZone", typeof(string));
                prev.Columns.Add("toZone", typeof(string));
                prev.Columns.Add("threshold", typeof(string));
                prev.Columns.Add("confidence", typeof(double));
                prev.Columns.Add("jobId", typeof(string));
                prev.Columns.Add("dockDoor", typeof(string));

                string fname = ConfigurationManager.AppSettings["ThresholdXmlFileName"];
                //To append to the file we must read the XML document back in first. 
                if (File.Exists(fname))
                    prev.ReadXml(fname);

                //Add the rows we just read from file to the rows we saw in this cycle
                for (int i = 0; i < thrRecs.Rows.Count; i++)
                    prev.Rows.Add(thrRecs.Rows[i].ItemArray);
                prev.AcceptChanges();

                using (StreamWriter sw = new StreamWriter(fname))
                    prev.WriteXml(sw);
            }
            catch (Exception ex)
            {
                string errMsg = "WriteThresholdRecordsToXML Exception: " + ex.Message + "(" + ex.GetType() + ")";
                if (null != ex.InnerException)
                    errMsg += Environment.NewLine + ex.InnerException.Message;
                iLog.WriteEntry(errMsg, EventLogEntryType.Error, eventId); eventId++;
            }

            #if (DEBUG)
            #region debug_WriteThresholdRecordsToXML_kpi
                        DateTime procTmEnd = DateTime.Now;
                        TimeSpan procTmSpan = procTmEnd.Subtract(blockTmSt);
                        iLog.WriteEntry("WriteThresholdRecordsToXML completed(ms): " + procTmSpan.Milliseconds.ToString(), EventLogEntryType.Information, eventId); eventId++;
            #endregion
            #endif

        }

        private void WriteThresholdRecordsToCSV()
        {
            #if (DEBUG)
            #region debug_WriteThresholdRecordsToCSV_kpi
                        DateTime blockTmSt = System.DateTime.Now;
                        iLog.WriteEntry("WriteThresholdRecordsToCSV started: " + blockTmSt.ToLongTimeString(), EventLogEntryType.Information, eventId); eventId++;
            #endregion
            #endif

            try
            {
                string fname = ConfigurationManager.AppSettings["ThresholdCsvFileName"];
                foreach (ThresholdRec rec in thrRecords)
                    WriteToCSV(fname, rec.ThresholdRecToCsvString(), RecordType.Threshold);
            }
            catch (Exception ex)
            {
                string errMsg = "WriteThresholdRecordsToCSV Exception: " + ex.Message + "(" + ex.GetType() + ")";
                if (null != ex.InnerException)
                    errMsg += Environment.NewLine + ex.InnerException.Message;
                iLog.WriteEntry(errMsg, EventLogEntryType.Error, eventId); eventId++;
            }

            #if (DEBUG)
            #region debug_WriteThresholdRecordsToCSV_kpi
                        DateTime procTmEnd = DateTime.Now;
                        TimeSpan procTmSpan = procTmEnd.Subtract(blockTmSt);
                        iLog.WriteEntry("WriteThresholdRecordsToCSV completed(ms): " + procTmSpan.Milliseconds.ToString(), EventLogEntryType.Information, eventId); eventId++;
            #endregion
            #endif

        }

        private static void WriteToCSV(string fname, string message, RecordType recType)
        {
            string msg = string.Empty;
            try
            {
                if (!File.Exists(fname))
                {
                    //Write Header values for the file based upon record type
                    switch (recType)
                    {
                        case RecordType.ItemEvent: msg = "epc,tag_id,job_id,from_zone,from_floor,to_zone,to_floor,from_facility,to_facility,from_x,from_y,to_x,to_y,observation_time"; break;
                        case RecordType.Threshold: msg = "epc,observation_time,from_zone,to_zone,threshold,confidence,job_id,job_name"; break;
                        default: break;
                    }
                }

                using (StreamWriter sw = File.AppendText(fname))
                {
                    if (msg.Length > 0)
                        sw.WriteLine(msg + "\n" + message);
                    else
                        sw.WriteLine(message);
                }
            }
            catch (Exception ex)
            {
                string errMsg = "WriteToCSV Exception: " + ex.Message + "(" + ex.GetType() + ")";
                if (null != ex.InnerException)
                    errMsg += Environment.NewLine + ex.InnerException.Message;
                iLog.WriteEntry(errMsg, EventLogEntryType.Error, eventId); eventId++;
            }
        }

        private static void WriteRawItemEventRecordsToXML()
        {
            #if (DEBUG)
            #region debug_WriteRawItemEventRecordsToXML_kpi
                        DateTime blockTmSt = System.DateTime.Now;
                        iLog.WriteEntry("WriteRawItemEventRecordsToXML started: " + blockTmSt.ToLongTimeString(), EventLogEntryType.Information, eventId); eventId++;
            #endregion
            #endif

            try
            {
                DataTable prev = new DataTable("is_item_event");
                prev.Columns.Add("epc", typeof(string));
                prev.Columns.Add("tagId", typeof(string));
                prev.Columns.Add("jobId", typeof(string));
                prev.Columns.Add("fromZone", typeof(string));
                prev.Columns.Add("fromFloor", typeof(string));
                prev.Columns.Add("toZone", typeof(string));
                prev.Columns.Add("toFloor", typeof(string));
                prev.Columns.Add("fromFacility", typeof(string));
                prev.Columns.Add("toFacility", typeof(string));
                prev.Columns.Add("fromX", typeof(double));
                prev.Columns.Add("fromY", typeof(double));
                prev.Columns.Add("toX", typeof(double));
                prev.Columns.Add("toY", typeof(double));
                prev.Columns.Add("observationTime", typeof(DateTime));

                string fname = ConfigurationManager.AppSettings["RawItemEventXmlFileName"];
                //To append to the file we must read the XML document back in first. 
                if (File.Exists(fname))
                    prev.ReadXml(fname);

                //Add the rows we just read from file to the rows we saw in this cycle
                for (int i = 0; i < rawItemEventRecs.Rows.Count; i++)
                    prev.Rows.Add((rawItemEventRecs.Rows[i].ItemArray));
                prev.AcceptChanges();

                using (StreamWriter sw = new StreamWriter(fname))
                    prev.WriteXml(sw);
            }
            catch (Exception ex)
            {
                string errMsg = "WriteRawItemEventRecordsToXML Exception: " + ex.Message + "(" + ex.GetType() + ")";
                if (null != ex.InnerException)
                    errMsg += Environment.NewLine + ex.InnerException.Message;
                iLog.WriteEntry(errMsg, EventLogEntryType.Error, eventId); eventId++;
            }

            #if (DEBUG)
            #region debug_WriteRawItemEventRecordsToXML_kpi
                        DateTime procTmEnd = DateTime.Now;
                        TimeSpan procTmSpan = procTmEnd.Subtract(blockTmSt);
                        iLog.WriteEntry("WriteRawItemEventRecordsToXML completed(ms): " + procTmSpan.Milliseconds.ToString(), EventLogEntryType.Information, eventId); eventId++;
            #endregion
            #endif
        }

        private static void WriteRawItemEventRecordsToCSV()
        {
            #if (DEBUG)
            #region debug_WriteRawItemEventRecordsToCSV_kpi
                        DateTime blockTmSt = System.DateTime.Now;
                        iLog.WriteEntry("WriteRawItemEventRecordsToCSV started: " + blockTmSt.ToLongTimeString(), EventLogEntryType.Information, eventId); eventId++;
            #endregion
            #endif

            try
            {
                string fname = ConfigurationManager.AppSettings["RawItemEventCsvFileName"];
                foreach (ItemEventRec rec in itemEventRecords)
                    WriteToCSV(fname, rec.ItemEventRecToCsvString(), RecordType.ItemEvent);
            }
            catch(Exception ex)
            {
                string errMsg = "WriteRawItemEventRecordsToCSV Exception: " + ex.Message + "(" + ex.GetType() + ")";
                if (null != ex.InnerException)
                    errMsg += Environment.NewLine + ex.InnerException.Message;
                iLog.WriteEntry(errMsg, EventLogEntryType.Error, eventId); eventId++;
            }

            #if (DEBUG)
            #region debug_WriteRawItemEventRecordsToCSV_kpi
                        DateTime procTmEnd = DateTime.Now;
                        TimeSpan procTmSpan = procTmEnd.Subtract(blockTmSt);
                        iLog.WriteEntry("WriteRawItemEventRecordsToCSV completed(ms): " + procTmSpan.Milliseconds.ToString(), EventLogEntryType.Information, eventId); eventId++;
            #endregion
            #endif
        }


        /// <summary>
        /// Moves AMQP threshold records retrieved into in-memory table
        /// </summary>
        /// <returns></returns>
        public bool WriteThresholdRecordsToTable()
        {
            #if (DEBUG)
            #region debug_WriteThresholdRecordsToTable_kpi
                        DateTime blockTmSt = System.DateTime.Now;
                        iLog.WriteEntry("WriteThresholdRecordsToTable started: " + blockTmSt.ToLongTimeString(), EventLogEntryType.Information, eventId); eventId++;
            #endregion
            #endif

            bool retVal = true;

            try
            {
                #region DataTable Setup definitions

                thrRecs = new DataTable("is_thresh");

                DataColumn epcColumn = new DataColumn();
                epcColumn.DataType = typeof(String);
                epcColumn.ColumnName = "epc";
                thrRecs.Columns.Add(epcColumn);

                DataColumn obsTmColumn = new DataColumn();
                obsTmColumn.DataType = typeof(DateTime);
                obsTmColumn.ColumnName = "observationTime";
                thrRecs.Columns.Add(obsTmColumn);

                DataColumn fromColumn = new DataColumn();
                fromColumn.DataType = typeof(String);
                fromColumn.ColumnName = "fromZone";
                thrRecs.Columns.Add(fromColumn);

                DataColumn toColumn = new DataColumn();
                toColumn.DataType = typeof(String);
                toColumn.ColumnName = "toZone";
                thrRecs.Columns.Add(toColumn);

                DataColumn threshColumn = new DataColumn();
                threshColumn.DataType = typeof(String);
                threshColumn.ColumnName = "threshold";
                thrRecs.Columns.Add(threshColumn);

                DataColumn confColumn = new DataColumn();
                confColumn.DataType = typeof(Double);
                confColumn.ColumnName = "confidence";
                thrRecs.Columns.Add(confColumn);

                DataColumn jobIdColumn = new DataColumn();
                jobIdColumn.DataType = typeof(String);
                jobIdColumn.ColumnName = "jobId";
                thrRecs.Columns.Add(jobIdColumn);

                DataColumn doorColumn = new DataColumn();
                doorColumn.DataType = typeof(String);
                doorColumn.ColumnName = "dockDoor";
                thrRecs.Columns.Add(doorColumn);

#endregion

                foreach (ThresholdRec rec in thrRecords)
                    thrRecs.Rows.Add(rec.Epc, rec.ObservationTime, rec.FromZone, rec.ToZone, rec.Threshold, rec.Confidence, rec.JobId, rec.JobName);
            }
            catch (Exception ex)
            {
                string errMsg = "WriteThresholdRecordsToTable Exception: " + ex.Message + "(" + ex.GetType() + ")";
                if (null != ex.InnerException)
                    errMsg += Environment.NewLine + ex.InnerException.Message;
                iLog.WriteEntry(errMsg, EventLogEntryType.Error, eventId); eventId++;
                retVal = false;
            }

            #if (DEBUG)
            #region debug_WriteThresholdRecordsToTable_kpi
                        DateTime procTmEnd = DateTime.Now;
                        TimeSpan procTmSpan = procTmEnd.Subtract(blockTmSt);
                        iLog.WriteEntry("WriteThresholdRecordsToTable completed(ms): " + procTmSpan.Milliseconds.ToString(), EventLogEntryType.Error, eventId); eventId++;
            #endregion
            #endif

            return retVal;
        }

        /// <summary>
        /// Moves AMQP item event records retrieved into in-memory table
        /// </summary>
        /// <returns></returns>
        static bool WriteRawItemEventRecordsToTable()
        {
            bool retVal = true;

            #if (DEBUG)
            #region debug_WriteRawItemEventRecordsToTable_kpi
                        DateTime blockTmSt = System.DateTime.Now;
                        iLog.WriteEntry("WriteRawItemEventRecordsToTable started: " + blockTmSt.ToLongTimeString(), EventLogEntryType.Information, eventId); eventId++;
            #endregion
            #endif

            try
            {

                #region DataTable Setup definitions

                rawItemEventRecs = new DataTable("is_raw_item_events");

                DataColumn epcColumn = new DataColumn();
                epcColumn.DataType = typeof(String);
                epcColumn.ColumnName = "epc_nbr";
                rawItemEventRecs.Columns.Add(epcColumn);

                DataColumn tagIdColumn = new DataColumn();
                tagIdColumn.DataType = typeof(String);
                tagIdColumn.ColumnName = "tag_id";
                rawItemEventRecs.Columns.Add(tagIdColumn);

                DataColumn jobIdColumn = new DataColumn();
                jobIdColumn.DataType = typeof(String);
                jobIdColumn.ColumnName = "job_id";
                rawItemEventRecs.Columns.Add(jobIdColumn);

                DataColumn fromZoneColumn = new DataColumn();
                fromZoneColumn.DataType = typeof(String);
                fromZoneColumn.ColumnName = "from_zone";
                rawItemEventRecs.Columns.Add(fromZoneColumn);

                DataColumn fromFloorColumn = new DataColumn();
                fromFloorColumn.DataType = typeof(String);
                fromFloorColumn.ColumnName = "from_floor";
                rawItemEventRecs.Columns.Add(fromFloorColumn);

                DataColumn toZoneColumn = new DataColumn();
                toZoneColumn.DataType = typeof(String);
                toZoneColumn.ColumnName = "to_zone";
                rawItemEventRecs.Columns.Add(toZoneColumn);

                DataColumn toFloorColumn = new DataColumn();
                toFloorColumn.DataType = typeof(String);
                toFloorColumn.ColumnName = "to_floor";
                rawItemEventRecs.Columns.Add(toFloorColumn);

                DataColumn fromFacColumn = new DataColumn();
                fromFacColumn.DataType = typeof(String);
                fromFacColumn.ColumnName = "from_facility";
                rawItemEventRecs.Columns.Add(fromFacColumn);

                DataColumn toFacColumn = new DataColumn();
                toFacColumn.DataType = typeof(String);
                toFacColumn.ColumnName = "to_facility";
                rawItemEventRecs.Columns.Add(toFacColumn);

                DataColumn fromXColumn = new DataColumn();
                fromXColumn.DataType = typeof(double);
                fromXColumn.ColumnName = "from_x";
                rawItemEventRecs.Columns.Add(fromXColumn);

                DataColumn fromYColumn = new DataColumn();
                fromYColumn.DataType = typeof(double);
                fromYColumn.ColumnName = "from_y";
                rawItemEventRecs.Columns.Add(fromYColumn);

                DataColumn toXColumn = new DataColumn();
                toXColumn.DataType = typeof(double);
                toXColumn.ColumnName = "to_x";
                rawItemEventRecs.Columns.Add(toXColumn);

                DataColumn toYColumn = new DataColumn();
                toYColumn.DataType = typeof(double);
                toYColumn.ColumnName = "to_y";
                rawItemEventRecs.Columns.Add(toYColumn);

                DataColumn obsTmColumn = new DataColumn();
                obsTmColumn.DataType = typeof(DateTime);
                obsTmColumn.ColumnName = "observation_time";
                rawItemEventRecs.Columns.Add(obsTmColumn);

#endregion

                foreach (ItemEventRec rec in itemEventRecords)
                    rawItemEventRecs.Rows.Add(rec.Epc, rec.TagId, rec.JobId, rec.FromZone, rec.FromFloor, rec.ToZone, rec.ToFloor, rec.FromFacility, rec.ToFacility, rec.FromX, rec.FromY, rec.ToX, rec.ToY, rec.ObservationTime);
            }
            catch (Exception ex)
            {
                string errMsg = "WriteRawItemEventRecordsToTable Exception: " + ex.Message + "(" + ex.GetType() + ")";
                if (null != ex.InnerException)
                    errMsg += Environment.NewLine + ex.InnerException.Message;
                iLog.WriteEntry(errMsg, EventLogEntryType.Error, eventId); eventId++;
                retVal = false;
            }

            #if (DEBUG)
            #region debug_WriteRawItemEventRecordsToTable_kpi
                        DateTime procTmEnd = DateTime.Now;
                        TimeSpan procTmSpan = procTmEnd.Subtract(blockTmSt);
                        iLog.WriteEntry("WriteRawItemEventRecordsToTable completed(ms): " + procTmSpan.Milliseconds.ToString(), EventLogEntryType.Information, eventId); eventId++;
            #endregion
            #endif

            return retVal;
        }

        public static string GetCustomUpc(string tagId)
        {
            string retVal = tagId.Substring(0, 11);

            try
            {
                string pre4 = tagId.Substring(0, 4);
                string pre7 = tagId.Substring(0, 7);
                string pre9 = tagId.Substring(0, 9);
                string pre11 = tagId.Substring(0, 11);

                foreach (ItemFileRec rec in itemFileRecords)
                {
                    if (rec.Upc == pre4)
                        return rec.Upc;
                    else if (rec.Upc == pre7)
                        return rec.Upc;
                    else if (rec.Upc == pre9)
                        return rec.Upc;
                    else if (rec.Upc == pre11)
                        return rec.Upc;
                }
            }
            catch (Exception ex)
            {
                string errMsg = "Exception: " + ex.Message + "(" + ex.GetType() + ")";
                if (null != ex.InnerException)
                    errMsg += Environment.NewLine + ex.InnerException.Message;
                iLog.WriteEntry(errMsg, EventLogEntryType.Error, eventId); eventId++;
                return retVal;
            }

            return retVal;
        }
    }
}
