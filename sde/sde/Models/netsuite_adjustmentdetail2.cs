//------------------------------------------------------------------------------
// <auto-generated>
//    This code was generated from a template.
//
//    Manual changes to this file may cause unexpected behavior in your application.
//    Manual changes to this file will be overwritten if the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

namespace sde.Models
{
    using System;
    using System.Collections.Generic;
    
    public partial class netsuite_adjustmentdetail2
    {
        public int nad_id { get; set; }
        public string nad_shipmentNo { get; set; }
        public string nad_ISBN13 { get; set; }
        public string nad_ISBN10 { get; set; }
        public Nullable<decimal> nad_receivedQty { get; set; }
        public Nullable<decimal> nad_usdCost { get; set; }
        public Nullable<decimal> nad_localCost { get; set; }
        public Nullable<decimal> nad_totalAmt { get; set; }
        public string nad_imasItemID { get; set; }
        public string nad_imasLocation { get; set; }
        public string nad_nsLocation { get; set; }
        public Nullable<int> nad_nsLocationID { get; set; }
        public Nullable<int> nad_nsItemID { get; set; }
        public Nullable<System.DateTime> nad_firstShipmentDate { get; set; }
        public string nad_type { get; set; }
        public string nad_businessChannel { get; set; }
        public Nullable<int> nad_postingPeriodID { get; set; }
    }
}