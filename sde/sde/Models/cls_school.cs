using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Web.Mvc;
using System.Web.Security;

namespace MVC4_WEBApi_Angular_CRUD.Models
{
    public class cls_school
    {
        public Int32? school_id { get; set; }
        public String school_name { get; set; }
        public String school_address { get; set; }
        public String school_email { get; set; }
    }

    public class schoolList
    {
        public List<cls_school> school { get; set; }
    }

}