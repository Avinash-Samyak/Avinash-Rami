using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Web.Mvc;
using System.Web.Security;

namespace sde.Models
{
    public class cls_incSOFulfillExport
    {
        public DateTime? Last_Fulfilled_Date { get; set; }
        public String SO_Number { get; set; }
        public String Customer { get; set; }
        public String Subsidiary { get; set; }
        public String Order_Pack { get; set; }
        public String Pack_Title { get; set; }
        public Int32? Order_Quantity { get; set; }
        public Int32? Order_Fulfilled { get; set; }
    }

    public class incSOFulfillExportReport
    {
        public List<cls_incSOFulfillExport> iSOFEr = new List<cls_incSOFulfillExport>();
    }
}