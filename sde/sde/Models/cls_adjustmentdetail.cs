using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Web.Mvc;
using System.Web.Security;
using System.Linq;

namespace sde.Models
{
    public class cls_adjustmentdetail
    {
        public String userID { get; set; }
        public String firstName { get; set; }
        public String lastName { get; set; }
        public String email { get; set; }
        public String userName { get; set; }
        public String password { get; set; }
        public String createdBy { get; set; }
        public DateTime? modifiedDate { get; set; }
        public DateTime? updatedDate { get; set; }
        public String role { get; set; }

        public Boolean isValid (string userName, string password)
        {
            try
            {
                using (sdeEntities entities = new sdeEntities())
                {
                    var valid = (from row in entities.users
                                 where row.u_userName == userName &&
                                 row.u_password == password
                                 select row).ToList();
                    if (valid.Count > 0)
                        return true;
                    else
                        return false;

                }
            }
            catch (Exception ex)
            {
                return false;
            }
        }

        public String[] GetRole(string userName)
        {
            try
            {
                using (sdeEntities entities = new sdeEntities())
                {
                    var valid = (from row in entities.users
                                 where row.u_userName == userName
                                 select row.u_role).ToList();

                    string[] roles = valid[0].Split(',');
                    return roles;
                }
            }
            catch (Exception ex)
            {
                string[] roles = new string[1];
                return roles;
            }
            
        }

    }

   
  
    
}