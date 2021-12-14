using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Web.Mvc;
using System.Web.Security;

namespace sde.Models
{
    public class cls_sorReceive
    {
        public String schID { get; set; }
        public String number { get; set; }
        public DateTime? returnDate { get; set; }
        //public DateTime? createdDate { get; set; }    // for testing
        public String invoice { get; set; }
        public Int32? numOfItems { get; set; }
        public Int32? sum_receive_qty { get; set; }
        
    }

    public class cls_sorReceiveItem
    {
        public String schID { get; set; }
        public String number { get; set; }
        public DateTime? returnDate { get; set; }
        //public DateTime? createdDate { get; set; }    // for testing
        public String invoice { get; set; }
        public String isbn { get; set; }
        public String item_description { get; set; }
        public Int32? receive_qty { get; set; }
    }

    public class sorReceiveList
    {
        public List<cls_sorReceive> sorReceive { get; set; }
    }

    public class sorReceiveItemList
    {
        public List<cls_sorReceiveItem> sorReceiveItem { get; set; }
    }
}