using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Web.Mvc;
using System.Web.Security;
using System.Linq;

namespace sde.Models
{
    public class cls_nsdataformq
    {
        public String transactionType { get; set; }
        public String jobID { get; set; }
        public String internalID { get; set; }
        public String consolidateTable { get; set; }
        public String status { get; set; }
        public DateTime? pushDate { get; set; }
        public DateTime? pullDate { get; set; }
        public DateTime? updatedDate { get; set; }
        public String tranID { get; set; }
    }

    public class NSDataForMqList
    {
        public List<cls_nsdataformq> list { get; set; }
    }

  
    
}