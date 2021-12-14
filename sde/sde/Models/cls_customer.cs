using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Web.Mvc;
using System.Web.Security;
using System.Linq;

namespace sde.Models
{
    public class cls_customer
    {
        public String custID { get; set; }  //Customer Internal ID(If dropshipment will based on SG Sales Order Customer Info)
        public String addressee { get; set; }
        public String deliveryAdd { get; set; }
        public String deliveryAdd2 { get; set; }
        public String deliveryAdd3 { get; set; }
        public String postCode { get; set; }
        public String contactPerson { get; set; }
        public String phone { get; set; }
        public String country { get; set; }
        public String customerID { get; set; } //Customer ID (If dropshipment will based on MY Sales Order Customer Info) - WY-22.SEPT.2014
        public String customerInternalID { get; set; } //Customer Internal ID (If dropshipment will based on MY Sales Order Customer Info) - WY-22.SEPT.2014
        
        //Added Billing Address - WY-25.SEPT.2014
        public String billingAddressee { get; set; }
        public String billingAdd { get; set; }
        public String billingAdd2 { get; set; }
        public String billingAdd3 { get; set; }
        public String billingPostcode { get; set; }
        public String billingContactPerson { get; set; }
        public String billingPhone { get; set; }  
    }

    public class RequestCustomerList
    {
        public List<cls_customer> error { get; set; }
    }

  
    
}