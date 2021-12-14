using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Web.Mvc;
using System.Web.Security;
using System.Linq;

namespace sde.Models
{
    public class cls_listbox
    {
        [Display(Name = "New String:")]
        public String NewText { get; set; }

        public IEnumerable<SelectListItem> ListText { get; set; }
        public IEnumerable<string> SelectedListText { get; set; }
    }
    public class cls_listbox_Operation
    {
        public string validateInput(string _newText)
        {
            string strErrMessage = "";

            if (_newText.IndexOf("=") < 1)
            {
                strErrMessage = "The string has no equal sign!";
            }
            else if (!System.Text.RegularExpressions.Regex.IsMatch(_newText.Replace("=", "").Replace(" ", ""), @"^[a-zA-Z0-9]+$"))
            {
                strErrMessage = "The string is not an alphanumeric string!";
            }

            return strErrMessage;
        }
        public List<SelectListItem> addIntoListBoxes(Dictionary<string, string> _dicListItems, string _newText)
        {
            List<SelectListItem> lstSelectListItems = new List<SelectListItem>();
            if (_newText != null)
            {
                _dicListItems.Add(System.Guid.NewGuid().ToString(), _newText);
            }

            foreach (KeyValuePair<string, string> kyListItem in _dicListItems)
            {
                SelectListItem selectItem = new SelectListItem()
                {
                    Text = kyListItem.Value,
                    Value = kyListItem.Value,
                    Selected = true
                };

                lstSelectListItems.Add(selectItem);
            }

            return lstSelectListItems;
        }
        public List<SelectListItem> sortByName(Dictionary<string, string> _dicListItems)
        {
            List<SelectListItem> lstSelectListItems = new List<SelectListItem>();
            if (_dicListItems.Count > 0)
            {
                //Sort the dictionary by Name
                var LinListItems = from item in _dicListItems
                                   orderby item.Value ascending
                                   select item;

                foreach (KeyValuePair<string, string> kyListItem in LinListItems)
                {
                    SelectListItem selectItem = new SelectListItem()
                    {
                        Text = kyListItem.Value,
                        Value = kyListItem.Value,
                        Selected = true
                    };

                    lstSelectListItems.Add(selectItem);
                }
            }

            return lstSelectListItems;
        }
        public List<SelectListItem> sortByValue(Dictionary<string, string> _dicListItems)
        {
            List<SelectListItem> lstSelectListItems = new List<SelectListItem>();
            if (_dicListItems.Count > 0)
            {
                //Sort the dictionary by Name
                var LinListItems = from item in _dicListItems
                             orderby item.Value.Substring(item.Value.IndexOf("="), (item.Value.Length - item.Value.IndexOf("="))) ascending
                             select item;

                foreach (KeyValuePair<string, string> kyListItem in LinListItems)
                {
                    SelectListItem selectItem = new SelectListItem()
                    {
                        Text = kyListItem.Value,
                        Value = kyListItem.Value,
                        Selected = true
                    };

                    lstSelectListItems.Add(selectItem);
                }
            }

            return lstSelectListItems;
        }
    }
}