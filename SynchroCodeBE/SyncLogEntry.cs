using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SynchroCodeBE
{
    public class SyncLogEntry
    {
        public int ID { get; set; }
        public string TableName { get; set; }
        public string Operation { get; set; }
        public string KeyValues { get; set; }
        public string NewValues { get; set; }
        public string OldValues { get; set; }
    }

}
