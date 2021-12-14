using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Web.Mvc;
using System.Web.Security;

namespace sde.Models
{
    public class cls_scheduler
    {
        public String category { get; set; }
        public String transactionType { get; set; }
        public String status { get; set; }
        public String minuteGap { get; set; }
        public DateTime? nextRun { get; set; }
        public Int32? nextRunSeqNo { get; set; }
        public DateTime? lastRun { get; set; }
        public Int32? lastRunSeqNo { get; set; }
        public Int32? sequence { get; set; }
        public Int32? id { get; set; }
    }

    public class SchedulerList
    {
        public List<cls_scheduler> scheduler { get; set; }
    }

    public class cls_viewScheduler
    {
        public String category { get; set; }
        public String transactionType { get; set; }
        public String status { get; set; }
        public String minuteGap { get; set; }
        public String nextRun { get; set; }
        public Int32? nextRunSeqNo { get; set; }
        public String lastRun { get; set; }
        public Int32? lastRunSeqNo { get; set; }
        public Int32? sequence { get; set; }
        public Int32? id { get; set; }
    }
}