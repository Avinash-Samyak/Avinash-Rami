using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Web.Mvc;
using System.Web.Security;

namespace sde.Models
{
    public class cls_sorSyncExport
    {
        public DateTime? CreatedDate { get; set; }
        public String Return_Number { get; set; }
        public String Customer { get; set; }
        public String Invoice { get; set; }
        public String ISBN { get; set; }
        public String Item_Description { get; set; }
        public Int32? Return_Quantity { get; set; }
    }

    public class sorSyncExportReport
    {
        public List<cls_sorSyncExport> sorSEr = new List<cls_sorSyncExport>();
    }
}