using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Text;

namespace azureStoragesConsoleApp
{
    class Customer : TableEntity
    {
        public string Name { get; set; }
        public string Email { get; set; }

        public Customer(string name, string email)
        {
            this.Name = name;
            this.Email = email;
            this.PartitionKey = "US";
            this.RowKey = email;
        }
        public Customer()
        {

        }
    }
}