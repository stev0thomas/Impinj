// Copyright ©2018 Impinj, Inc. All rights reserved.
// You may use and modify this code under the terms of the Impinj Software Tools License & Disclaimer.
// Visit https://support.impinj.com/hc/en-us/articles/360000468370-Software-Tools-License-Disclaimer
// for full license details, or contact Impinj, Inc. at support@impinj.com for a copy of the license.

using System;
using Newtonsoft.Json;


namespace ItemSenseRDBMService
{
    class EpcMasterRec
    {
        [JsonProperty("epc", NullValueHandling = NullValueHandling.Ignore)]
        public string Epc { get; set; } = "ABSENT";

        [JsonProperty("lastObsvTime")]
        public DateTime ObservationTime { get; set; } = DateTime.Now;


        [JsonProperty("tagId", NullValueHandling = NullValueHandling.Ignore)]
        public string TagId { get; set; } = "ABSENT";


        [JsonProperty("zoneName", NullValueHandling = NullValueHandling.Ignore)]
        public string ZoneName { get; set; } = "ABSENT";

        [JsonProperty("floor", NullValueHandling = NullValueHandling.Ignore)]
        public string Floor { get; set; } = "ABSENT";

        [JsonProperty("facility", NullValueHandling = NullValueHandling.Ignore)]
        public string Facility { get; set; } = "ABSENT";


        [JsonProperty("xCoord", NullValueHandling = NullValueHandling.Ignore)]
        public double Xcoord { get; set; } = 0;

        [JsonProperty("yCoord", NullValueHandling = NullValueHandling.Ignore)]
        public double Ycoord { get; set; } = 0;


        [JsonProperty("upc", NullValueHandling = NullValueHandling.Ignore)]
        public string Upc { get; set; } = "ABSENT";

        [JsonProperty("lastUpdateTime")]
        public DateTime LastUpdateTime { get; set; } = DateTime.Now;

        public EpcMasterRec()
        {
        }

        public EpcMasterRec(string epc, DateTime observationTime, string tagId, string zoneName, string floor, string facility, double xCoord, 
            double yCoord, string upc, DateTime lastUpdateTime)
        {
            Epc = epc;
            ObservationTime = observationTime;
            TagId = tagId;
            ZoneName = zoneName;
            Floor = floor;
            Facility = facility;
            Xcoord = xCoord;
            Ycoord = yCoord;
            Upc = upc;
            LastUpdateTime = lastUpdateTime;
        }

        public string EpcMasterEventRecToCsvString()
        {
            return string.Format(
                "{0},{1},{2},{3},{4},{5},{6},{7},{8},{9}",
                Epc,
                ObservationTime,
                TagId,
                ZoneName,
                Floor,
                Facility,
                Xcoord,
                Ycoord,
                Upc,
                LastUpdateTime
                );
        }
    }
}
