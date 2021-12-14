using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Web.Mvc;
using System.Web.Security;
namespace sde.Models
{
    public class cls_sofulfillment
    {
        public DateTime? rangeTo { get; set; }
        public DateTime? exportDate { get; set; }
        public String moNo { get; set; }
        public String deliveryRef { get; set; }
        public String job_ID { get; set; }
        public String schID { get; set; }
        public String mono_internalID { get; set; }
        public String status { get; set; }
        public String errorDesc { get; set; }
    }

    public class cls_soFulfillmentItem
    {
        public DateTime? rangeTo { get; set; }
        public DateTime? exportDate { get; set; }
        public String moNo { get; set; }
        public String deliveryRef { get; set; }
        public String ordPack { get; set; }
        public String packTitle { get; set; }
        public Int32? ordFulfill { get; set; }

        // #605
        public Int32? ordQty { get; set; }
        public DateTime? lastFulfilledDate { get; set; }
        public String customer { get; set; }
        public String subsidiary { get; set; }
    }

    public class cls_incompleteSOFulfillment    // #605
    {
        public DateTime? rangeTo { get; set; }
        public String moNo { get; set; }
        public String custID { get; set; }
        public String customer { get; set; }
        public String subsidiary { get; set; }
        public DateTime? lastFulfilledDate { get; set; }
    }

    public class cls_BcOrderFulfillment
    {
        public String ordPack { get; set; }
        public String ordType { get; set; }
        public Decimal ordPrice { get; set; }
        public Decimal ordGst { get; set; }
        public Decimal ordShipping { get; set; }
        public Int32 ordQty { get; set; }
        public Int32 ordFulfill { get; set; }
        public Int32 ordFulfillnSku { get; set; }
    }

    public class cls_BcIncompleteOrderFulfillment
    {
        public String ordPack { get; set; }
        public String ordISBN { get; set; }
        public Int32 ordQty { get; set; }
        public Int32 ordFulfill { get; set; }
        public Int32 ordFulfillnSku { get; set; }
    }

    public class soFulfillmentList
    {
        public List<cls_sofulfillment> soFulfillment { get; set; }
    }

    public class soFulfillmentItemList
    {
        public List<cls_soFulfillmentItem> soFulfillmentItem { get; set; }
    }

    public class incompleteSOFulfillmentList    // #605
    {
        public List<cls_incompleteSOFulfillment> incompleteSOFulfillment { get; set; }
    }

    public class BcOrderFulfillmentList  
    {
        public List<cls_BcOrderFulfillment> BcOrderFulfillment { get; set; }
    }

    public class BcIncompleteOrderFulfillmentList
    {
        public List<cls_BcIncompleteOrderFulfillment> BcIncompleteOrderFulfillment { get; set; }
    }

}