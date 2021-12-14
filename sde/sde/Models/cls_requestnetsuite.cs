using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Web.Mvc;
using System.Web.Security;

namespace sde.Models
{
    public class cls_requestnetsuite
    {
        public String transactionType { get; set; }
        public String status { get; set; }
        public long? seqNo { get; set; }
        public String jobID { get; set; }
        public DateTime? rangeFrom { get; set; }
        public DateTime? rangeTo { get; set; }
        public DateTime? completedAt { get; set; }
        public DateTime? updatedDate { get; set; }
        public Int32? sequence { get; set; }
    }

    public class RequestNetsuiteList
    {
        public List<cls_requestnetsuite> requestnetsuite { get; set; }
    }
    
}