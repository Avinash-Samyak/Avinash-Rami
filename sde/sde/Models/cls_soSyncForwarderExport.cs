using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Web.Mvc;
using System.Web.Security;

namespace sde.Models
{
    public class cls_soSyncForwarderExport
    {
        public DateTime? Extract_Date { get; set; }
        public String SO_Number { get; set; }
        public String Internal_ID { get; set; }
        public String Customer_Name { get; set; }
        public String Address_1 { get; set; }
        public String Address_2 { get; set; }
        public String Address_3 { get; set; }
        public String Address_4 { get; set; }
        public String DeliveryType { get; set; }
        public String Tag { get; set; }
        public String Contact { get; set; }
        public String Tel { get; set; }
        public String Tel2 { get; set; }
        public String Fax { get; set; }
    }

    public class soSyncForwarderExportReport
    {
        public List<cls_soSyncForwarderExport> soSFEr = new List<cls_soSyncForwarderExport>();
    }
}