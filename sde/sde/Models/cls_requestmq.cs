using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Web.Mvc;
using System.Web.Security;

namespace sde.Models
{
    public class cls_requestmq
    {
        public String transactionType { get; set; }
        public String status { get; set; }
        public long? seqNo { get; set; }
        public String jobID { get; set; }
        public DateTime? rangeFrom { get; set; }
        public DateTime? rangeTo { get; set; }
        public DateTime? completedAt { get; set; }
        public Int32? sequence { get; set; }
    }

    public class RequestMQList
    {
        public List<cls_requestmq> requestmq { get; set; }
    }
    
}