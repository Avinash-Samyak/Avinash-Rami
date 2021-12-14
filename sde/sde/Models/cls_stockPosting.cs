using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Web.Mvc;
using System.Web.Security;

namespace sde.Models
{
    public class cls_stockPosting
    {
        public String transactionType { get; set; }
        public String sPID { get; set; }
        public String sDesc { get; set; }
        public Decimal? dQty { get; set; }
        public String sLoc { get; set; }
        public String subsidiary { get; set; }
        public DateTime? postingDate { get; set; }
        public DateTime? createdDate { get; set; }

    }

    public class stockPostingList
    {
        public List<cls_stockPosting> stockPosting { get; set; }
    }
} 