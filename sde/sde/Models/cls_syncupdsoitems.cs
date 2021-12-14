using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace sde.Models
{
    public class cls_syncupdsoitems 
    {
        public String soInternalID { get; set; }
        public String soItemInternalID { get; set; }  
        public Double tolQtyForWMS { get; set; } 
        public Double remainingQty { get; set; }
        public Double wmsfulfilledQty { get; set; }  
    }

    public class RequestSyncUpdSOList
    {
        public List<cls_syncupdsoitems> error { get; set; }
    }
}
