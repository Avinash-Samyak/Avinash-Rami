using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Web.Mvc;
using System.Web.Security;

namespace sde.Models
{
    public class cls_soSyncExport
    {
        public DateTime? Extract_Date { get; set; }
        public String SO_Number { get; set; }
        public String ISBN { get; set; }
        public String Item_Description { get; set; }
        public Int32? Quantity_For_WMS { get; set; }
        public String Customer_ID { get; set; }
        public String Customer_Name { get; set; }
        public String Country { get; set; }
        public String DeliveryAdd { get; set; }
        public String DeliveryAdd_2 { get; set; }
        public String DeliveryAdd_3 { get; set; }
        public String PostCode { get; set; }
        public String Delivery_Type { get; set; }
        public String Contact_Person { get; set; }
        public String TelNo { get; set; }
    }

    public class soSyncExportReport
    {
        public List<cls_soSyncExport> soSEr = new List<cls_soSyncExport>();
    }
}