using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Web.Mvc;
using System.Web.Security;

namespace sde.Models
{
    public class cls_soSyncPriceExport
    {
        public DateTime? Extract_Date { get; set; }
        public String SO_Number { get; set; }
        public String ISBN { get; set; }
        public String Item_Description { get; set; }
        public Int32? Order_Quantity { get; set; }
        public Double? Order_Rate { get; set; }
        public Double? Tax { get; set; }
        public Double? Discount { get; set; }
    }

    public class soSyncPriceExportReport
    {
        public List<cls_soSyncPriceExport> soSPEr = new List<cls_soSyncPriceExport>();
    }
}