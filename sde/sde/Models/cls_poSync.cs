using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Web.Mvc;
using System.Web.Security;

namespace sde.Models
{
    public class cls_poSync
    {
        public String pr_ID { get; set; }
        public String pr_number { get; set; }
        public String pr_supplier { get; set; }
        public String pr_location { get; set; }
        public DateTime? rangeTo { get; set; }
        public Int32? numOfItems { get; set; }
        public Int32? sum_pritemQty { get; set; }
        public Decimal? sum_pritemPrice { get; set; }
    }
    public class cls_poSyncItem
    {
        public String pr_ID { get; set; }
        public String pr_number { get; set; }
        public String pr_supplier { get; set; }
        public String pr_location { get; set; }
        public DateTime? rangeTo { get; set; }
        public String item_ID { get; set; }
        public String item_description { get; set; }
        public Int32? pritem_qty { get; set; }
        public Decimal? pritem_price { get; set; }
    }
    public class poSyncList
    {
        public List<cls_poSync> poSync { get; set; }
    }

    public class poSyncItemList
    {
        public List<cls_poSyncItem> poSyncItem { get; set; }
    }
}