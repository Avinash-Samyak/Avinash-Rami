using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using sde.Models;
using System.Collections;

namespace sde.Controllers
{
    public class HomeController : Controller
    {
        public ActionResult Index()
        {
            List<SelectListItem> listSelectListItems = new List<SelectListItem>();
            cls_listbox _listbox = new cls_listbox()
            {
                NewText = "",
                ListText = listSelectListItems,
                SelectedListText= null
            };
            return View(_listbox);
        }

        [HttpPost]
        public ActionResult Index(cls_listbox listboxMVC, string submitButton)
        {
            List<SelectListItem> listSelectListItems = new List<SelectListItem>();
            cls_listbox_Operation cListOper = new cls_listbox_Operation();

            switch (submitButton)
            {
                case "Add":
                    string strErrMsg = cListOper.validateInput(listboxMVC.NewText);
                    if (strErrMsg == "")
                    {
                        listSelectListItems = cListOper.addIntoListBoxes(getListBoxes(listboxMVC), listboxMVC.NewText);
                    }
                    else
                    {
                        ModelState.AddModelError("CustomError", strErrMsg);
                    }
                    break;
                case "Name":
                    listSelectListItems = cListOper.sortByName(getListBoxes(listboxMVC));
                    break;
                case "Value":
                    listSelectListItems = cListOper.sortByValue(getListBoxes(listboxMVC));
                    break;
                case "Delete":
                    listSelectListItems = new List<SelectListItem>();
                    break;
                default:
                    break;
            }

            cls_listbox _listbox = new cls_listbox()
            {
                NewText = "",
                ListText = listSelectListItems,
                SelectedListText = null
            };

            return View(_listbox);
        }

        private Dictionary<string, string> getListBoxes(cls_listbox listboxMVC)
        {
            var dicListItems = new Dictionary<string, string>();
            if (listboxMVC.SelectedListText != null)
            {
                foreach (string item in listboxMVC.SelectedListText)
                {
                    dicListItems.Add(System.Guid.NewGuid().ToString(), item.ToString());
                }
            }

            return dicListItems;
        }

        [HttpPost]
        public ActionResult DeleteListBox(IEnumerable _list)
        {
            object[] rtnResult = new object[1];
            rtnResult[0] = "SUCCESS";
            return Json(rtnResult, JsonRequestBehavior.AllowGet);
        }

        /*
        private List<SelectListItem> addIntoListBoxes(cls_listbox listboxMVC)
        {
            List<SelectListItem> listSelectListItems = new List<SelectListItem>();
            if (listboxMVC.SelectedListText != null)
            {
                listSelectListItems = getListBoxes(listboxMVC);
            }

            if (listboxMVC.NewText != null)
            {
                SelectListItem selectList = new SelectListItem()
                {
                    Text = listboxMVC.NewText,
                    Value = listboxMVC.NewText,
                    Selected = true
                };

                listSelectListItems.Add(selectList);
            }

            return listSelectListItems;
        }
        private List<SelectListItem> sortByName(cls_listbox listboxMVC)
        {
            List<SelectListItem> listSelectListItems = new List<SelectListItem>();
            if (listboxMVC.SelectedListText != null)
            {
                var result = from item in getListBoxes(listboxMVC)
                             orderby item.Text ascending
                             select item;

                foreach (SelectListItem selectListItem in result)
                {
                    SelectListItem selectList2 = new SelectListItem()
                    {
                        Text = selectListItem.Text,
                        Value = selectListItem.Value,
                        Selected = true
                    };
                    listSelectListItems.Add(selectList2);
                }
            }
            return listSelectListItems;
        }
        private List<SelectListItem> sortByValue(cls_listbox listboxMVC)
        {
            List<SelectListItem> listSelectListItems = new List<SelectListItem>();
            if (listboxMVC.SelectedListText != null)
            {
                var result = from item in getListBoxes(listboxMVC)
                             orderby item.Value.Substring(item.Value.IndexOf("="), (item.Value.Length - item.Value.IndexOf("="))) ascending
                             select item;

                foreach (SelectListItem selectListItem in result)
                {
                    SelectListItem selectList2 = new SelectListItem()
                    {
                        Text = selectListItem.Text,
                        Value = selectListItem.Value,
                        Selected = true
                    };
                    listSelectListItems.Add(selectList2);
                }
            }
            return listSelectListItems;
        }
        private List<SelectListItem> getListBoxes(cls_listbox listboxMVC)
        {
            List<SelectListItem> listSelectListItems = new List<SelectListItem>();
            foreach (string item in listboxMVC.SelectedListText)
            {
                SelectListItem selectList = new SelectListItem()
                {
                    Text = item.ToString(),
                    Value = item.ToString(),
                    Selected = true
                };
                listSelectListItems.Add(selectList);
            }

            return listSelectListItems;
        }
        */
    }
}
