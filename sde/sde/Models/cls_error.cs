using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Web.Mvc;
using System.Web.Security;

namespace sde.Models
{
    public class cls_error
    {
        public String taskName { get; set; }
        public String status { get; set; }
        public String jobID { get; set; }
        public String refNO { get; set; }
        public String taskDescription { get; set; }
        public String nsInternalId { get; set; }
        public String moInternalId { get; set; }
        public String errorDescription { get; set; }
        public DateTime? updatedDate { get; set; }
        public DateTime? createdDate { get; set; }
        public int taskID { get; set; }

    }

    public class RequestErrorList
    {
        public List<cls_error> error { get; set; }
    }
    
}