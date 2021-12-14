using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Entity;
using System.Globalization;
using System.Web.Security;

namespace sde.Models
{
    public class LoginModel2
    {
        [Required]
        [Display(Name = "LDAP User Name")]
        public string username { get; set; }

        [Required]
        [DataType(DataType.Password)]
        [Display(Name = "LDAP Password")]
        public string password { get; set; }

    }
}
