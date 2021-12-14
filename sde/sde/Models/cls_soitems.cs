using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Web.Mvc;
using System.Web.Security;
using System.Linq;

namespace sde.Models
{
    public class cls_soitems
    {
        public String soInternalID { get; set; }  
        public Double tolCommittedQty { get; set; }  
    }

    public class RequestSOList
    {
        public List<cls_soitems> error { get; set; }
    }
}
