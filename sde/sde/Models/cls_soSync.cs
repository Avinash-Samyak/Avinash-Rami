using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Web.Mvc;
using System.Web.Security;

namespace sde.Models
{
    public class cls_soSync
    {
        public DateTime? rangeTo { get; set; }
        //ANET-26 - Exception Report: Inbound & outbound Reports 
        public DateTime? sodate { get; set; }
        public String progressStatus { get; set; }
        public String moNo { get; set; }
        //ANET-26 - Exception Report: Inbound & outbound Reports 
        public String dropshipMono { get; set; }
        public String status { get; set; }
        public String description { get; set; }
        public String customer { get; set; }
        public String addressee { get; set; }
        public String country { get; set; }
        public Int32? numOfItems { get; set; }
        public Int32? sum_ordQty { get; set; }
        public Int32? sum_qtyForWMS { get; set; }
    }

    public class cls_soSyncItem
    {
        public DateTime? rangeTo { get; set; }
        public String progressStatus { get; set; }
        public String moNo { get; set; }
        public String customer { get; set; }
        public String addressee { get; set; }
        public String country { get; set; }
        public String itemID { get; set; }
        public String itemDescription { get; set; }
        public Int32? ordQty { get; set; }
        public Int32? qtyForWMS { get; set; }
    }

    public class cls_soPendingSync
    {
        public String progressStatus { get; set; }
        public DateTime? rangeTo { get; set; }
        public String moNo { get; set; }
        public String customer { get; set; }
        public String addressee { get; set; }
        public String country { get; set; }
        public Int32? ordQty { get; set; }
        public Int32? qtyForWMS { get; set; }

        public String totalItems { get; set; }
    }

    public class soSyncList
    {
        public List<cls_soSync> soSync { get; set; }
    }

    public class soSyncItemList
    {
        public List<cls_soSyncItem> soSyncItem { get; set; }
    }

    public class soPendingSyncList
    {
        public List<cls_soPendingSync> soPendingSync { get; set; }
    }

}