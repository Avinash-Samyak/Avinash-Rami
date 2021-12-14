using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Web.Mvc;
using System.Web.Security;

namespace sde.Models
{
    public class cls_poSyncExport
    {
        public DateTime? Extract_Date { get; set; }
        public String PO_Number { get; set; }
        public String Supplier { get; set; }
        public String Location { get; set; }
        public String ISBN { get; set; }
        public String Item_Description { get; set; }
        public Int32? Quantity { get; set; }
    }

    public class poSyncExportReport
    {
        public List<cls_poSyncExport> poSEr = new List<cls_poSyncExport>();
    }
}