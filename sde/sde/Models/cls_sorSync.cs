using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Web.Mvc;
using System.Web.Security;

namespace sde.Models
{
    public class cls_sorSync
    {
        public String schID { get; set; }
        public String number { get; set; }
        public DateTime? createdDate { get; set; }
        public String invoice { get; set; }
        public Int32? numOfItems { get; set; }
        public Int32? sum_return_qty { get; set; }
    }

    public class cls_sorSyncItem
    {
        public String schID { get; set; }
        public String number { get; set; }
        public DateTime? createdDate { get; set; }
        public String invoice { get; set; }
        public String isbn { get; set; }
        public String item_description { get; set; }
        public Int32? return_qty { get; set; }
    }

    public class sorSyncList
    {
        public List<cls_sorSync> sorSync { get; set; }
    }

    public class sorSyncItemList
    {
        public List<cls_sorSyncItem> sorSyncItem { get; set; }
    }
}