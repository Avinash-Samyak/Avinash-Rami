using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Web.Mvc;
using System.Web.Security;

namespace sde.Models
{
    public class cls_dashboard
    {
    }

    public class view_dashboard_salesorder
    {
        public String jobID { get; set; }
        public String jobMoID { get; set; }
        public String businessChannel { get; set; }
        public String country_tag { get; set; }
        public Int32? moCount { get; set; }
        public String moNo { get; set; }
        public String moInternalID { get; set; }
        public DateTime? rangeTo { get; set; }
    }

    public class DashboardSalesOrderList
    {
        public view_dashboard_salesorder dashboardSOSummary { get; set; }
        public List<view_dashboard_salesorder> dashboardSODetails { get; set; }
    }
    
}