// Copyright ©2018 Impinj, Inc. All rights reserved.
// You may use and modify this code under the terms of the Impinj Software Tools License & Disclaimer.
// Visit https://support.impinj.com/hc/en-us/articles/360000468370-Software-Tools-License-Disclaimer
// for full license details, or contact Impinj, Inc. at support@impinj.com for a copy of the license.

using System;
using Newtonsoft.Json;

namespace ItemSenseRDBMService
{
    class ItemFileRec
    {
 
        public ItemFileRec(string description, string upc)
        {
            Description = description;
            Upc = upc;
        }


        [JsonProperty("upc", NullValueHandling = NullValueHandling.Ignore)]
        public string Upc { get; set; } = "ABSENT";

        [JsonProperty("description", NullValueHandling = NullValueHandling.Ignore)]
        public string Description { get; set; } = "ABSENT";

    }
}
