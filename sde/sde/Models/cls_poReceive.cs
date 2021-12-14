using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Web.Mvc;
using System.Web.Security;

namespace sde.Models
{
    public class cls_poReceive
    {
        public DateTime? rangeTo { get; set; }
        public String prNumber { get; set; }
        public String internalID { get; set; }
        public String supplier { get; set; }
        public String location { get; set; }
        public String number { get; set; }
        public String invoice { get; set; }
        public Int32? numOfItems { get; set; }
    }

    public class cls_poReceiveItem
    {
        public DateTime? rangeTo { get; set; }
        public String prNumber { get; set; }
        public String number { get; set; }
        public String invoice { get; set; }
        public String location { get; set; }
        public String itemID { get; set; }
        public String itemDescription { get; set; }
        public Decimal? itemQty { get; set; }
        public Int32? damageQty { get; set; }
        public Int32? excessQty { get; set; }
    }
    
    public class poReceiveList
    {
        public List<cls_poReceive> poReceive { get; set; }
    }

    public class poReceiveItemList
    {
        public List<cls_poReceiveItem> poReceiveItem { get; set; }
    }
}