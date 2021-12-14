using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Web;
using DotNetOpenAuth.AspNet;
using Microsoft.Web.WebPages.OAuth;
using sde.Models;

namespace sde
{
    public static class AuthConfig
    {
        public static void RegisterAuth()
        {
            // To let users of this site log in using their accounts from other sites such as Microsoft, Facebook, and Twitter,
            // you must update this site. For more information visit http://go.microsoft.com/fwlink/?LinkID=252166

            //OAuthWebSecurity.RegisterMicrosoftClient(
            //clientId: "000000004C158B08",
            //clientSecret: "ZIwTsWVwsdDsdIiXROJe36yN-v0btwkr");

            /*
            OAuthWebSecurity.RegisterTwitterClient(
                consumerKey: "x",
                consumerSecret: "x");
            */

            /*
            OAuthWebSecurity.RegisterFacebookClient(
                appId: "1622924364612546",
                appSecret: "0409dfa105ec08a12b275b122ad699d9");

            OAuthWebSecurity.RegisterYahooClient();

            OAuthWebSecurity.RegisterLinkedInClient(
                consumerKey: "75s354w8d8pfol",
                consumerSecret: "U67Q63W80PUxnukU");
            */

            OAuthWebSecurity.RegisterClient(new GooglePlusClient("244640840049-nrrnmc2a0p53ch7ninaokl5lrh0nm42n.apps.googleusercontent.com", "gMDT0kt9v3PXmlg8w4OmTYTg"), "Google+", null);
        }
    }
}
