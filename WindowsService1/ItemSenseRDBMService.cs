// Copyright ©2018 Impinj, Inc. All rights reserved.
// You may use and modify this code under the terms of the Impinj Software Tools License & Disclaimer.
// Visit https://support.impinj.com/hc/en-us/articles/360000468370-Software-Tools-License-Disclaimer
// for full license details, or contact Impinj, Inc. at support@impinj.com for a copy of the license.

//#define DEBUG
#undef DEBUG

using System;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Timers;
using System.Runtime.InteropServices;
using System.Threading;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Collections;
using System.Configuration;
using System.IO;
using System.Net;
using ItemSenseRDBMService;

namespace ImpinjItemSenseRDBMService
{
    public partial class ItemSenseRDBMService : ServiceBase
    {
        [DllImport("advapi32.dll", SetLastError = true)]
        private static extern bool SetServiceStatus(IntPtr handle, ref ServiceStatus serviceStatus);

        private static int eventId = 1;
        private static AutoResetEvent exit_event = new AutoResetEvent(false);

        /// <summary>
        /// These are program arrays and thread handling events for AMQP reads between database insertions
        /// </summary>
        private static System.Collections.ArrayList g_itemEventRecords = null;
        private static System.Collections.ArrayList g_thrRecords = null;

        public static ArrayList g_itemFileRecords = null;

        private static Boolean waitOne = false;

        private static System.Diagnostics.EventLog iLog;

        public enum ServiceState
        {
            SERVICE_STOPPED = 0x00000001,
            SERVICE_START_PENDING = 0x00000002,
            SERVICE_STOP_PENDING = 0x00000003,
            SERVICE_RUNNING = 0x00000004,
            SERVICE_CONTINUE_PENDING = 0x00000005,
            SERVICE_PAUSE_PENDING = 0x00000006,
            SERVICE_PAUSED = 0x00000007,
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct ServiceStatus
        {
            public int dwServiceType;
            public ServiceState dwCurrentState;
            public int dwControlsAccepted;
            public int dwWin32ExitCode;
            public int dwServiceSpecificExitCode;
            public int dwCheckPoint;
            public int dwWaitHint;
        };


        public ItemSenseRDBMService()
        {
            InitializeComponent();
            iLog = new System.Diagnostics.EventLog();
            if (!System.Diagnostics.EventLog.SourceExists("ItemSense RDBMS Service"))
            {
                System.Diagnostics.EventLog.CreateEventSource(
                    "ItemSense RDBMS Service", "Impinj IS RDBMS Service");
            }
            iLog.Source = "ItemSense RDBMS Service";
            iLog.Log = "Impinj IS RDBMS Service";

            g_itemEventRecords = new ArrayList();
            g_thrRecords = new ArrayList();
            g_itemFileRecords = new ArrayList();
        }

 

        protected override void OnStart(string[] args)
        {
            //For debugging purposes only
            #if (DEBUG)
                System.Diagnostics.Debugger.Launch();
            #endif

            iLog.WriteEntry("ItemSense RDBMS OnStart called", EventLogEntryType.Information, eventId); eventId++;

            // Update the service state to Start Pending.  
            ServiceStatus serviceStatus = new ServiceStatus();
            serviceStatus.dwCurrentState = ServiceState.SERVICE_START_PENDING;
            serviceStatus.dwWaitHint = 100000;
            SetServiceStatus(this.ServiceHandle, ref serviceStatus);

            //Populate Item File from csv file if necessary
            PopulateItemFile();

            //Start threads to set up the AMQP Message port with the filters and settings from app.config
            //Instantiate the array lists that will hold the item, threshold
            //and item_event records read in the facility between processing times

            //kick off thread to get Item Event Reads
            Thread itemThread = new Thread(new ThreadStart(InitiateItemEventAMQP));
            itemThread.Start();

            //kick off thread to get Threshold Mode Reads
            Thread threshThread = new Thread(new ThreadStart(InitiateThresholdAMQP));
            threshThread.Start();

            // Set up a timer to trigger database inserts to short term tables.  
            System.Timers.Timer timer = new System.Timers.Timer();
            timer.Interval = Convert.ToInt32(ConfigurationManager.AppSettings["EventProcessingInterval(msecs)"]);
            timer.Elapsed += new ElapsedEventHandler(OnTimer);
            timer.Start();

            // Set up a timer to trigger last read location insert to long term tables.  
            System.Timers.Timer dbTimer = new System.Timers.Timer();
            dbTimer.Interval = Convert.ToInt32(ConfigurationManager.AppSettings["ItemSenseEventProcessingHistoryInterval(msecs)"]);
            dbTimer.Elapsed += new ElapsedEventHandler(OnDbTimer);
            dbTimer.Start();

            // Update the service state to Running.  
            serviceStatus.dwCurrentState = ServiceState.SERVICE_RUNNING;
            SetServiceStatus(this.ServiceHandle, ref serviceStatus);
        }

        private void PopulateItemFile()
        {
            try
            {
                string line;
                using (StreamReader reader = new StreamReader(ConfigurationManager.AppSettings["CustomItemFilterCsvFileName"]))
                {
                    line = reader.ReadLine();
                    string[] parms = line.Split(',');
                    ItemFileRec rec = new ItemFileRec(parms[0], parms[1]);
                    g_itemFileRecords.Add(rec);
                }
            }
            catch(Exception ex)
            {
                string errMsg = "Exception: " + ex.Message + "(" + ex.GetType() + ")";
                if (null != ex.InnerException)
                    errMsg += Environment.NewLine + ex.InnerException.Message;
                iLog.WriteEntry(errMsg, EventLogEntryType.Error, eventId); eventId++;
            }
        }

        private void OnDbTimer(object sender, ElapsedEventArgs e)
        {
            #if (DEBUG)
            #region debug_rdbs_event_kpi
                        DateTime blockTmSt = System.DateTime.Now;
                        iLog.WriteEntry("RDBMS DbTimer started: " + blockTmSt.ToLongTimeString(), EventLogEntryType.Information, eventId); eventId++;

            #endregion
            #endif

            try
            {
                //Extract, Transform, Load and Truncate processes will all be done via DDL scripts in specific RDBMS class
                string dbms = ConfigurationManager.AppSettings["TypeRDBMS"];
                switch (dbms.ToUpper())
                {
                    case "POSTGRESQL":
                        {
                            PostgreSqlRDBMS pgres = new PostgreSqlRDBMS(new ArrayList(), new ArrayList(), new ArrayList(), g_itemFileRecords);
                            pgres.ProcessItemSenseMessages();
                            break;
                        }
                    case "SQLSERVER":
                        {
                            SQLServerRDBMS sqlsrv = new SQLServerRDBMS(new ArrayList(), new ArrayList(), new ArrayList(), g_itemFileRecords);
                            sqlsrv.ProcessItemSenseMessages();
                            break;
                        }
                    default:
                        {
                            iLog.WriteEntry("App.config has incorrect database name defined.  POSTGRESQL or SQLSERVER are only valid options currently...",
                                EventLogEntryType.Error, eventId); eventId++;

                            break;
                        }
                }
            }
            catch (Exception ex)
            {
                string errMsg = "Exception: " + ex.Message + "(" + ex.GetType() + ")";
                if (null != ex.InnerException)
                    errMsg += Environment.NewLine + ex.InnerException.Message;
                iLog.WriteEntry(errMsg, EventLogEntryType.Error, eventId); eventId++;
            }

            #if (DEBUG)
            #region debug_rdbs_event_kpi
                        DateTime procTmEnd = DateTime.Now;
                        TimeSpan procTmSpan = procTmEnd.Subtract(blockTmSt);
                        iLog.WriteEntry("RDBMS DbTimer Processing completed(ms): " + procTmSpan.Milliseconds.ToString(), EventLogEntryType.Information, eventId); eventId++;

            #endregion
            #endif 

        }

        private static void InitiateThresholdAMQP()
        {
            // Define an object that will contain the AMQP Message Queue details
            ItemSense.AmqpMessageQueueDetails MsgQueueDetails = null;

            try
            {
                // Create a JSON object for configuring a Threshold Transition
                // Message Queue
                ThresholdTransitionMessageQueueConfig msgQConfig = new ThresholdTransitionMessageQueueConfig();
                msgQConfig.Threshold = ConfigurationManager.AppSettings["ThresholdTransitionThresholdFilter"];
                msgQConfig.JobId = ConfigurationManager.AppSettings["ThresholdTransitionJobIdFilter"];

                // Create a string-based JSON object of the object
                string objectAsJson = JsonConvert.SerializeObject(msgQConfig);
                // Now translate the JSON into bytes
                byte[] objectAsBytes = Encoding.UTF8.GetBytes(objectAsJson);

                // Create the full path to the configure Threshold Transitions
                // Message Queu endpoint from default ItemSense URI
                string ThresholdTransitionMessageQueueConfigApiEndpoint = ConfigurationManager.AppSettings["ItemSenseUri"] +
                    "/data/v1/items/queues/threshold";

                // Create a WebRequest, identifying it as a PUT request
                // with a JSON payload, and assign it the specified
                // credentials.
                WebRequest ItemSensePutRequest =
                     WebRequest.Create(ThresholdTransitionMessageQueueConfigApiEndpoint);

                ItemSensePutRequest.Credentials = new System.Net.NetworkCredential(ConfigurationManager.AppSettings["ItemSenseUsername"],
                    ConfigurationManager.AppSettings["ItemSensePassword"]);
                ItemSensePutRequest.Proxy = null;
                ItemSensePutRequest.Method = "PUT";
                ItemSensePutRequest.ContentType = "application/json";
                ItemSensePutRequest.ContentLength = objectAsBytes.Length;

                // Create an output data stream representation of the
                // POST WebRequest to output the data
                Stream dataStream = ItemSensePutRequest.GetRequestStream();
                dataStream.Write(objectAsBytes, 0, objectAsBytes.Length);
                dataStream.Close();

                // Execute the PUT request and retain the response.
                using (HttpWebResponse ItemSenseResponse = (HttpWebResponse)ItemSensePutRequest.GetResponse())
                {
                    // The response StatusCode is a .NET Enum, so convert it to
                    // integer so that we can verify it against the status
                    // codes that ItemSense returns
                    ItemSense.ResponseCode ResponseCode = (ItemSense.ResponseCode)ItemSenseResponse.StatusCode;

                    // In this instance, we are only interested in whether
                    // the ItemSense response to the PUT request was a "Success".
                    if (ItemSense.ResponseCode.Success == ResponseCode)
                    {
                        // Open a stream to access the response data which
                        // contains the AMQP URL and queue identifier
                        Stream ItemSenseDataStream = ItemSenseResponse.GetResponseStream();

                        // Only continue if an actual response data stream exists
                        if (null != ItemSenseDataStream)
                        {
                            // Create a StreamReader to access the resulting data
                            StreamReader objReader = new StreamReader(ItemSenseDataStream);

                            // Read the complete PUT request results as a raw string
                            string itemSenseData = objReader.ReadToEnd();

                            // Now convert the raw JSON into a 
                            // AmqpMessageQueueDetails class
                            // representation
                            MsgQueueDetails = JsonConvert.DeserializeObject<ItemSense.AmqpMessageQueueDetails>(itemSenseData);

                            MsgQueueDetails.ServerUrl = MsgQueueDetails.ServerUrl.Replace(":5672/%2F", string.Empty);

                            string infoMsg = "Message Queue details: " + Environment.NewLine + "URI: " + MsgQueueDetails.ServerUrl +
                                Environment.NewLine + "QueueID: " + MsgQueueDetails.Queue;
                            iLog.WriteEntry(infoMsg, EventLogEntryType.Information, eventId); eventId++;


                            // Close the data stream. If we have got here,
                            // everything has gone well and there are no
                            // errors.
                            ItemSenseDataStream.Close();
                        }
                        else
                        {
                            iLog.WriteEntry("null ItemSense data stream.", EventLogEntryType.Error, eventId); eventId++;

                        }
                    }
                    else
                    {
                        throw (new Exception(string.Format("ItemSense PUT Response returned status of {0}", ItemSenseResponse.StatusCode)));
                    }
                }

                // Now that we have our MessageQueue, we can create a RabbitMQ
                // factory to handle connections to ItemSense AMQP broker
                ConnectionFactory factory = new ConnectionFactory()
                {
                    Uri = MsgQueueDetails.ServerUrl,
                    AutomaticRecoveryEnabled = true,
                    UserName = ConfigurationManager.AppSettings["ItemSenseUsername"],
                    Password = ConfigurationManager.AppSettings["ItemSensePassword"]
                };

                // Now connect to the ItemSense factory
                using (var connection = factory.CreateConnection())

                // Create a fresh channel to handle message queue interactions
                using (var channel = connection.CreateModel())
                {
                    // Create an event consumer to receive incoming events
                    EventingBasicConsumer consumer = new EventingBasicConsumer(channel);
                    // Bind an event handler to the message received event
                    consumer.Received += Threshold_Received;

                    // Initiate consumption of data from the ItemSense queue
                    channel.BasicConsume(queue: MsgQueueDetails.Queue, noAck: true, consumer: consumer);

                    // Hang on here until exit_event is signaled
                    exit_event.WaitOne();
                }
            }
            catch (Exception ex)
            {
                string errMsg = "Exception: " + ex.Message + "(" + ex.GetType() + ")";
                if (null != ex.InnerException)
                    errMsg += Environment.NewLine + ex.InnerException.Message;
                iLog.WriteEntry(errMsg, EventLogEntryType.Error, eventId); eventId++;
            }
        }


        private static void InitiateItemEventAMQP()
        {
            // Define an object that will contain the AMQP Message Queue details
            ItemSense.AmqpMessageQueueDetails MsgQueueDetails = null;

            try
            {
                // Create a JSON object for configuring a zoneTransition
                // Message Queue
                ZoneTransitionMessageQueueConfig msgQConfig = new ZoneTransitionMessageQueueConfig();
                if (ConfigurationManager.AppSettings["ZoneTransitionDistanceFilter"].Length > 0)
                    msgQConfig.Distance = Convert.ToInt32(ConfigurationManager.AppSettings["ZoneTransitionDistanceFilter"]);
                if (ConfigurationManager.AppSettings["ZoneTransitionJobIdFilter"].Length > 0)
                    msgQConfig.JobId = ConfigurationManager.AppSettings["ZoneTransitionJobIdFilter"];
                if (ConfigurationManager.AppSettings["ZoneTransitionFromZoneFilter"].Length > 0)
                    msgQConfig.FromZone = ConfigurationManager.AppSettings["ZoneTransitionFromZoneFilter"];
                if (ConfigurationManager.AppSettings["ZoneTransitionToZoneFilter"].Length > 0)
                    msgQConfig.ToZone = ConfigurationManager.AppSettings["ZoneTransitionToZoneFilter"];
                if (ConfigurationManager.AppSettings["ZoneTransitionFromFacilityFilter"].Length > 0)
                    msgQConfig.FromFacility = ConfigurationManager.AppSettings["ZoneTransitionFromFacilityFilter"];
                if (ConfigurationManager.AppSettings["ZoneTransitionToFacilityFilter"].Length > 0)
                    msgQConfig.ToFacility = ConfigurationManager.AppSettings["ZoneTransitionToFacilityFilter"];
                if (ConfigurationManager.AppSettings["ZoneTransitionEpcFilter"].Length > 0)
                    msgQConfig.EPC = ConfigurationManager.AppSettings["ZoneTransitionEpcFilter"];
                if (ConfigurationManager.AppSettings["ZoneTransitionsOnlyFilter"].Length > 0)
                    msgQConfig.ZoneTransitionsOnly = Convert.ToBoolean(ConfigurationManager.AppSettings["ZoneTransitionsOnlyFilter"]);

                // Create a string-based JSON object of the object
                string objectAsJson = JsonConvert.SerializeObject(msgQConfig);
                // Now translate the JSON into bytes
                byte[] objectAsBytes = Encoding.UTF8.GetBytes(objectAsJson);

                // Create the full path to the configure zoneTransitions
                // Message Queu endpoint from default ItemSense URI
                string ZoneTransitionMessageQueueConfigApiEndpoint =
                    ConfigurationManager.AppSettings["ItemSenseUri"] +
                    "/data/v1/items/queues";

                // Create a WebRequest, identifying it as a PUT request
                // with a JSON payload, and assign it the specified
                // credentials.
                WebRequest ItemSensePutRequest =
                     WebRequest.Create(ZoneTransitionMessageQueueConfigApiEndpoint);

                ItemSensePutRequest.Credentials =
                    new System.Net.NetworkCredential(
                        ConfigurationManager.AppSettings["ItemSenseUsername"],
                        ConfigurationManager.AppSettings["ItemSensePassword"]
                        );
                ItemSensePutRequest.Proxy = null;
                ItemSensePutRequest.Method = "PUT";
                ItemSensePutRequest.ContentType = "application/json";
                ItemSensePutRequest.ContentLength = objectAsBytes.Length;

                // Create an output data stream representation of the
                // POST WebRequest to output the data
                Stream dataStream = ItemSensePutRequest.GetRequestStream();
                dataStream.Write(objectAsBytes, 0, objectAsBytes.Length);
                dataStream.Close();

                // Execute the PUT request and retain the response.
                using (HttpWebResponse ItemSenseResponse = (HttpWebResponse)ItemSensePutRequest.GetResponse())
                {
                    // The response StatusCode is a .NET Enum, so convert it to
                    // integer so that we can verify it against the status
                    // codes that ItemSense returns
                    ItemSense.ResponseCode ResponseCode =
                        (ItemSense.ResponseCode)ItemSenseResponse.StatusCode;

                    // In this instance, we are only interested in whether
                    // the ItemSense response to the PUT request was a "Success".
                    if (ItemSense.ResponseCode.Success == ResponseCode)
                    {
                        // Open a stream to access the response data which
                        // contains the AMQP URL and queue identifier
                        Stream ItemSenseDataStream = ItemSenseResponse.GetResponseStream();

                        // Only continue if an actual response data stream exists
                        if (null != ItemSenseDataStream)
                        {
                            // Create a StreamReader to access the resulting data
                            StreamReader objReader = new StreamReader(ItemSenseDataStream);

                            // Read the complete PUT request results as a raw string
                            string itemSenseData = objReader.ReadToEnd();

                            // Now convert the raw JSON into a 
                            // AmqpMessageQueueDetails class
                            // representation
                            MsgQueueDetails =
                                JsonConvert.DeserializeObject<ItemSense.AmqpMessageQueueDetails>(
                                itemSenseData
                                );

                            MsgQueueDetails.ServerUrl = MsgQueueDetails.ServerUrl.Replace(":5672/%2F", string.Empty);

                            string infoMsg = "Message Queue details: " + Environment.NewLine + "URI: " + MsgQueueDetails.ServerUrl + Environment.NewLine + "QueueID: " + MsgQueueDetails.Queue;
                            iLog.WriteEntry(infoMsg, EventLogEntryType.Information, eventId); eventId++;


                            // Close the data stream. If we have got here,
                            // everything has gone well and there are no
                            // errors.
                            ItemSenseDataStream.Close();
                        }
                        else
                        {
                            iLog.WriteEntry("null ItemSense data stream.", EventLogEntryType.Error, eventId); eventId++;
                        }
                    }
                    else
                    {
                        throw (new Exception(string.Format(
                            "ItemSense PUT Response returned status of {0}",
                            ItemSenseResponse.StatusCode
                            )));
                    }
                }

                // Now that we have our MessageQueue, we can create a RabbitMQ
                // factory to handle connections to ItemSense AMQP broker
                ConnectionFactory factory = new ConnectionFactory()
                {
                    Uri = MsgQueueDetails.ServerUrl,
                    AutomaticRecoveryEnabled = true,
                    UserName = ConfigurationManager.AppSettings["ItemSenseUsername"],
                    Password = ConfigurationManager.AppSettings["ItemSensePassword"]
                };

                // Now connect to the ItemSense factory
                using (var connection = factory.CreateConnection())

                // Create a fresh channel to handle message queue interactions
                using (var channel = connection.CreateModel())
                {
                    // Create an event consumer to receive incoming events
                    EventingBasicConsumer consumer =
                        new EventingBasicConsumer(channel);
                    // Bind an event handler to the message received event
                    consumer.Received += ItemEvent_Received;

                    // Initiate consumption of data from the ItemSense queue
                    channel.BasicConsume(queue: MsgQueueDetails.Queue,
                                         noAck: true,
                                         consumer: consumer);


                    // Hang on here until exit_event is signaled
                    exit_event.WaitOne();
                }
            }
            catch (Exception ex)
            {
                string errMsg = "Exception: " + ex.Message + "(" + ex.GetType() + ")";
                if (null != ex.InnerException)
                    errMsg += Environment.NewLine + ex.InnerException.Message;
                iLog.WriteEntry(errMsg, EventLogEntryType.Error, eventId); eventId++;
            }
        }

 
 
        /// <summary>
        /// Message received event handler - AMQP Callback for Threshold Events
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void Threshold_Received(object sender, BasicDeliverEventArgs e)
        {
            #if (DEBUG)
            #region debug_amqp_event_kpi
                        DateTime blockTmSt = System.DateTime.Now;
                        iLog.WriteEntry("AMQP Message Received: " + blockTmSt.ToLongTimeString(), EventLogEntryType.Information, eventId); eventId++;

            #endregion
            #endif

            if (waitOne)
            {
                do
                    Thread.Sleep(50);
                while (waitOne);
            }

            #if (DEBUG)
            #region debug_amqp_event_kpi
                        DateTime blockTmEnd = System.DateTime.Now;
                        TimeSpan blockSpan = blockTmEnd.Subtract(blockTmSt);
                        iLog.WriteEntry("AMQP Queue WaitTime(ms): " + blockSpan.TotalMilliseconds.ToString(), EventLogEntryType.Information, eventId); eventId++;

            #endregion
            #endif

            var body = e.Body;
            var message = Encoding.UTF8.GetString(body);
            try
            {
                string msg = message.Replace("\\", string.Empty);
                string cln = msg.Replace("\"", string.Empty);
                string[] msgFields = cln.Split(',');
                int recCnt = msgFields.Count();
                switch (recCnt)
                {
                    case 8:  //It's a threshold job record
                        {
                            ThresholdRec rec = new ThresholdRec();
                            for (int i = 0; i < msgFields.Count(); i++)
                            {
                                string[] parms = null;
                                switch (i)
                                {
                                    case 0: parms = msgFields[i].Split(':'); rec.Epc = parms[1]; break;
                                    case 1:
                                        {
                                            string x = msgFields[i].Replace("observationTime:", string.Empty);
                                            rec.ObservationTime = Convert.ToDateTime(x);
                                            break;
                                        }
                                    case 2: parms = msgFields[i].Split(':'); rec.FromZone = parms[1]; break;
                                    case 3: parms = msgFields[i].Split(':'); rec.ToZone = parms[1]; break;
                                    case 4: parms = msgFields[i].Split(':'); rec.Threshold = parms[1]; break;
                                    case 5:
                                        {
                                            parms = msgFields[i].Split(':');
                                            if (parms[1].Length > 0 & parms[1] != @"null")
                                                rec.Confidence = Convert.ToDouble(parms[1]);
                                            break;
                                        }
                                    case 6: parms = msgFields[i].Split(':'); rec.JobId = parms[1]; break;
                                    case 7:
                                        {
                                            parms = msgFields[i].Split(':');
                                            string x = parms[1];
                                            rec.DockDoor = x.Replace("}", string.Empty);
                                            break;
                                        }
                                    default: break;
                                }
                            }
                            g_thrRecords.Add(rec);
                            string deb = rec.ThresholdRecToCsvString();
                            if (Convert.ToBoolean(ConfigurationManager.AppSettings["ShowTagsInEventViewer"]))
                                iLog.WriteEntry("Threshold Event Received: " + deb, EventLogEntryType.Information, eventId); eventId++;

                            break;
                        }
                    default:
                        {
                            iLog.WriteEntry("Unexpected number of fields received in Threshold AMQP Event Handler.  Expected 8 Received " + msgFields.Count(),
                                EventLogEntryType.Error, eventId); eventId++;
                            break;
                        }
                }
            }
            catch (Exception ex)
            {
                string errMsg = "Exception: " + ex.Message + "(" + ex.GetType() + ")";
                if (null != ex.InnerException)
                    errMsg += Environment.NewLine + ex.InnerException.Message;
                iLog.WriteEntry(errMsg, EventLogEntryType.Error, eventId); eventId++;
            }

            #if (DEBUG)
            #region debug_amqp_event_kpi
                        DateTime procEndTm = DateTime.Now;
                        TimeSpan procTmSpan = procEndTm.Subtract(blockTmEnd);
                        iLog.WriteEntry("Received: " + message + " Completed(ms): " + procTmSpan.TotalMilliseconds.ToString(), EventLogEntryType.Information, eventId); eventId++;

            #endregion
            #endif
        }

        /// <summary>
        /// Message received event handler - AMQP Callback Item and Item Events
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void ItemEvent_Received(object sender, BasicDeliverEventArgs e)
        {
            #if (DEBUG)
            #region debug_amqp_event_kpi
                        DateTime blockTmSt = System.DateTime.Now;
                        iLog.WriteEntry("AMQP Message Received: " + blockTmSt.ToLongTimeString(), EventLogEntryType.Information, eventId); eventId++;

            #endregion
            #endif

            if (waitOne)
            {
                do
                    Thread.Sleep(50);
                while (waitOne);
            }

            #if (DEBUG)
            #region debug_amqp_event_kpi
                        DateTime blockTmEnd = System.DateTime.Now;
                        TimeSpan blockSpan = blockTmEnd.Subtract(blockTmSt);
                        iLog.WriteEntry("AMQP Queue WaitTime(ms): " + blockSpan.TotalMilliseconds.ToString(), EventLogEntryType.Information, eventId); eventId++;

            #endregion
            #endif

            var body = e.Body;
            var message = Encoding.UTF8.GetString(body);

            try
            {
                string msg = message.Replace("\\", string.Empty);
                string cln = msg.Replace("\"", string.Empty);
                string[] msgFields = cln.Split(',');
                int recCnt = msgFields.Count();
                switch (recCnt)
                {
                    case 14:  //It's an Item Event record
                        {
                            ItemEventRec rec = new ItemEventRec();
                            for (int i = 0; i < msgFields.Count(); i++)
                            {
                                string[] parms = null;
                                switch (i)
                                {
                                    case 0: parms = msgFields[i].Split(':'); rec.Epc = parms[1]; break;
                                    case 1: parms = msgFields[i].Split(':'); rec.TagId = parms[1]; break;
                                    case 2: parms = msgFields[i].Split(':'); rec.JobId = parms[1]; break;
                                    case 3: parms = msgFields[i].Split(':'); rec.FromZone = parms[1]; break;
                                    case 4: parms = msgFields[i].Split(':'); rec.FromFloor = parms[1]; break;
                                    case 5: parms = msgFields[i].Split(':'); rec.ToZone = parms[1]; break;
                                    case 6: parms = msgFields[i].Split(':'); rec.ToFloor = parms[1]; break;
                                    case 7: parms = msgFields[i].Split(':'); rec.FromFacility = parms[1]; break;
                                    case 8: parms = msgFields[i].Split(':'); rec.ToFacility = parms[1]; break;
                                    case 9:
                                        {
                                            parms = msgFields[i].Split(':');
                                            if (parms[1].Length > 0 & parms[1] != @"null")
                                                rec.FromX = Convert.ToDouble(parms[1]);
                                            break;
                                        }
                                    case 10:
                                        {
                                            parms = msgFields[i].Split(':');
                                            if (parms[1].Length > 0 & parms[1] != @"null")
                                                rec.FromY = Convert.ToDouble(parms[1]);
                                            break;
                                        }
                                    case 11:
                                        {
                                            parms = msgFields[i].Split(':');
                                            if (parms[1].Length > 0 & parms[1] != @"null")
                                                rec.ToX = Convert.ToDouble(parms[1]);
                                            break;
                                        }
                                    case 12:
                                        {
                                            parms = msgFields[i].Split(':');
                                            if (parms[1].Length > 0 & parms[1] != @"null")
                                                rec.ToY = Convert.ToDouble(parms[1]);
                                            break;
                                        }
                                    case 13:
                                        {
                                            string y = msgFields[i].Replace("observationTime:", string.Empty);
                                            rec.ObservationTime = Convert.ToDateTime(y.Replace("}", string.Empty));
                                            break;
                                        }
                                    default: break;
                                }
                            }
                            g_itemEventRecords.Add(rec);
                            string deb = rec.ItemEventRecToCsvString();
                            if (Convert.ToBoolean(ConfigurationManager.AppSettings["ShowTagsInEventViewer"]))
                                iLog.WriteEntry("Item Event Received: " + deb, EventLogEntryType.Information, eventId); eventId++;

                            break;
                        }
                    default:
                        {
                            iLog.WriteEntry("Unexpected number of fields received in Item_Event AMQP Event Handler.  Expected 14 Received " + msgFields.Count(),
                                EventLogEntryType.Error, eventId); eventId++;

                            break;
                        }
                }
            }
            catch (Exception ex)
            {
                string errMsg = "Exception: " + ex.Message + "(" + ex.GetType() + ")";
                if (null != ex.InnerException)
                    errMsg += Environment.NewLine + ex.InnerException.Message;
                iLog.WriteEntry(errMsg, EventLogEntryType.Error, eventId); eventId++;
            }

            #if (DEBUG)
            #region debug_amqp_event_kpi
                        DateTime procEndTm = DateTime.Now;
                        TimeSpan procTmSpan = procEndTm.Subtract(blockTmEnd);
                        iLog.WriteEntry("Received: " + message + " Completed(ms): " + procTmSpan.TotalMilliseconds.ToString(), EventLogEntryType.Information, eventId); eventId++;

            #endregion
            #endif
        }

        private void OnTimer(object sender, ElapsedEventArgs e)
        {
            #if (DEBUG)
            #region debug_rdbs_event_kpi
                        DateTime blockTmSt = System.DateTime.Now;
                        iLog.WriteEntry("RDBMS timer started: " + blockTmSt.ToLongTimeString(), EventLogEntryType.Information, eventId); eventId++;

            #endregion
            #endif

            try
            {
                if (g_itemEventRecords.Count > 0 || g_thrRecords.Count > 0)
                {
                    //Block the Item, ItemEvent, and Threshold Callbacks from processing any more records
                    waitOne = true;

                    //Copy the Array Lists into one of the RDBMS or Base RDBMS class and clear the global ones
                    string dbms = ConfigurationManager.AppSettings["TypeRDBMS"];
                    switch (dbms.ToUpper())
                    {
                        case "POSTGRESQL":
                            {
                                PostgreSqlRDBMS pgres = new PostgreSqlRDBMS(new ArrayList(), g_itemEventRecords, g_thrRecords, g_itemFileRecords);
                                g_itemEventRecords.Clear();
                                g_thrRecords.Clear();
                                //Unblock the ItemEvent and Threshold Callbacks
                                waitOne = false;
                                pgres.ProcessAMQPmessages();
                                break;
                            }
                        case "SQLSERVER":
                            {
                                SQLServerRDBMS sqlsrv = new SQLServerRDBMS(new ArrayList(), g_itemEventRecords, g_thrRecords, g_itemFileRecords);
                                g_itemEventRecords.Clear();
                                g_thrRecords.Clear();
                                //Unblock the ItemEvent and Threshold Callbacks
                                waitOne = false;
                                sqlsrv.ProcessAMQPmessages();
                                break;
                            }
                        default:
                            {
                                RDBMSbase rdbms = new RDBMSbase(new ArrayList(), g_itemEventRecords, g_thrRecords, g_itemFileRecords);
                                g_itemEventRecords.Clear();
                                g_thrRecords.Clear();
                                //Unblock the ItemEvent and Threshold Callbacks
                                waitOne = false;
                                rdbms.ProcessAMQPmessages();
                                break;
                            }
                    }
                }
            }
            catch (Exception ex)
            {
                string errMsg = "Exception: " + ex.Message + "(" + ex.GetType() + ")";
                if (null != ex.InnerException)
                    errMsg += Environment.NewLine + ex.InnerException.Message;
                iLog.WriteEntry(errMsg, EventLogEntryType.Error, eventId); eventId++;

            }

            #if (DEBUG)
            #region debug_rdbs_event_kpi
                        DateTime copyTmEnd = System.DateTime.Now;
                        TimeSpan copyTmSpan = copyTmEnd.Subtract(blockTmSt);
                        iLog.WriteEntry("Deep array copy completed(ms): " + copyTmSpan.Milliseconds.ToString(), EventLogEntryType.Information, eventId); eventId++;
            #endregion
            
            #region debug_rdbs_event_kpi
                        DateTime procTmEnd = DateTime.Now;
                        TimeSpan procTmSpan = procTmEnd.Subtract(blockTmSt);
                        iLog.WriteEntry("RDBMS Processing completed(ms): " + procTmSpan.Milliseconds.ToString(), EventLogEntryType.Information, eventId); eventId++;

            #endregion
            #endif
        }

        protected override void OnStop()
        {
            iLog.WriteEntry("ItemSense RDBMS OnStop called", EventLogEntryType.Information, eventId++);

            ServiceStatus serviceStatus = new ServiceStatus();
            serviceStatus.dwCurrentState = ServiceState.SERVICE_STOP_PENDING;
            serviceStatus.dwWaitHint = 100000;
            SetServiceStatus(this.ServiceHandle, ref serviceStatus);

            //Notify thread to stop processing AMQP Messages
            exit_event.Set();

            // Update the service state to Stopped.  
            serviceStatus.dwCurrentState = ServiceState.SERVICE_STOPPED;
            SetServiceStatus(this.ServiceHandle, ref serviceStatus);
        }

        protected override void OnContinue()
        {
            iLog.WriteEntry("ItemSense RDBMS OnContinue called", EventLogEntryType.Information, eventId++);
        }

        protected override void OnPause()
        {
            iLog.WriteEntry("ItemSense RDBMS OnPause called", EventLogEntryType.Information, eventId++);
        }

        protected override void OnShutdown()
        {
            iLog.WriteEntry("ItemSense RDBMS OnShutdown called", EventLogEntryType.Information, eventId++);
        }
    }
}
