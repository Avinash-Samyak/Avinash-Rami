using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Web.Mvc;
using System.Web.Security;
using System.Linq;

namespace sde.Models
{
    public class cls_dummySO
    {
        public string dummySOID { get; set; }
        public String itemInternalID { get; set; }
        public String originalQty { get; set; }
        public String newQty { get; set; }
        public String ISBN { get; set; }
        public String title { get; set; }
        public String item_ID { get; set; }
        public String subsidiaryID { get; set; }
        public String businessChannelID { get; set; }
        public DateTime? pushtoNSDate { get; set; }
        public DateTime? pullFromWMSDate { get; set; }
        public Int32 deductedQty { get; set; }
    }

    public class dummySOList
    {
        public List<cls_dummySO> dummySO { get; set; }
    }

  
    
}