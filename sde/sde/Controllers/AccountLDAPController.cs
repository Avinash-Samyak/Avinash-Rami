using System;
using System.Collections.Generic;
using System.Linq;
using System.Transactions;
using System.Web;
using System.Web.Mvc;
using System.Web.Security;
using System.DirectoryServices.AccountManagement;
using DotNetOpenAuth.AspNet;
using Microsoft.Web.WebPages.OAuth;
using WebMatrix.WebData;
using sde.Filters;
using sde.Models;

namespace sde.Controllers
{
    public class AccountLDAPController : Controller
    {
        //
        // GET: /Login/

        public ActionResult Index()
        {
            return View();
        }

        [HttpPost]
        [AllowAnonymous]
        [ValidateAntiForgeryToken]
        public ActionResult Index(LoginModel2 objL, string returnUrl)
        {
            bool isValid;
            if (ModelState.IsValid)
            {
                string username = objL.username;
                string password = objL.password;

                try {
                    using (PrincipalContext pc = new PrincipalContext(ContextType.Domain, "SCHOLASTICARIT"))
                    {
                        isValid = pc.ValidateCredentials(username, password);
                    }
                } 
                catch (Exception ex)
                {
                    ModelState.AddModelError("", ex.Message.ToString());
                    isValid = false;
                }

                if (isValid)
                {
                    if (WebSecurity.Login(username, password))
                    { }
                    else
                    {
                        if (!WebSecurity.UserExists(username))
                        {
                            WebSecurity.CreateUserAndAccount(username, password);
                        }
                        WebSecurity.Login(username, password);
                    }

                    MySqlMembership mbr = new MySqlMembership();
                    if (mbr.ValidateOAuthLogin(username))
                    {
                        FormsAuthentication.SetAuthCookie(username, false);
                        TempData["Message"] = username;
                        return RedirectToAction("Main", "DashboardMain");
                    }
                    else
                    {
                        return RedirectToAction("ExternalLoginFailure");
                    }              
                }
            }

            ModelState.AddModelError("", "The LDAP user name or LDAP password provided is incorrect.");
            return View(objL);
        }
    }
}
