using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Web.Mvc;
using System.Web.Security;

namespace sde.Models
{
    public class cls_summary
    {
    }

    public class cls_cpasSummaryCount
    {
        public String transactionType { get; set; }
        public String description { get; set; }
        public String subsidiary { get; set; }
        public Int32? numOfTransaction { get; set; }
    }

    public class cpasSummaryCountList
    {
        public List<cls_cpasSummaryCount> cpasSummaryCount { get; set; }
    }
}