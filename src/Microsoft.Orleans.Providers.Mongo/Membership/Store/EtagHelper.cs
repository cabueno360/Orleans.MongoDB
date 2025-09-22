using System;

namespace Microsoft.Orleans.Providers.Mongo.Membership.Store
{
    public static class EtagHelper
    {
        public static string CreateNew()
        {
            return Guid.NewGuid().ToString();
        }
    }
}
