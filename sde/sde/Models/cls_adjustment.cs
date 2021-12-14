using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Web.Mvc;
using System.Web.Security;
using System.Linq;

namespace sde.Models
{
    public class cls_adjustment
    {
        public String id { get; set; }
        public String analysisCode { get; set; }
        public String shipmentNo { get; set; }
        public String firstShipmentDate { get; set; }
        public String nas_Type { get; set; }
        public String nassd { get; set; }
        public String createdBy { get; set; }
        public DateTime? modifiedDate { get; set; }
        public DateTime? updatedDate { get; set; }
        public String role { get; set; }
    }

 

  
    
}