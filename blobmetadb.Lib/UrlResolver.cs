using System;
using System.Net;
using System.Web;
using System.Linq;
using System.Collections.Specialized;

namespace BlobMetaDb.Lib
{
	/// <summary>
	/// Helps understanding Blob Storage Request URIs.
	/// </summary>
	public class UrlResolver
	{
		internal const string RootContainer = "$root";

		private Uri Address { get; set; }

		private NameValueCollection Parameters { get; set; }

		public UrlResolver (Uri addr)
		{
			this.Address = addr;
			this.Parameters = HttpUtility.ParseQueryString (addr.Query);
		}

		/// <returns><c>true</c> if identifies a container; not a blob</returns>
		public bool IsContainerRef {
			get {
				return Parameters ["restype"] == "container";
			}
		}

		/// <returns><c>true</c> if identifies a blob; not a container</returns>
		public bool IsBlobRef {
			get {
				// Corner case: The following could be a blob on $root container:
				// 		PUT https://myaccount.blob.core.windows.net/myblob?comp=metadata
				// Also the following request is not a container ref, but not a blob ref:
				//		GET https://myaccount.blob.core.windows.net/?comp=list
				var parts = Address.AbsolutePath.Split ('/');
				return !IsContainerRef && (parts.Length >= 2 && parts [1] != "");
			}
		}

		public bool IsCompNone {
			get{ return Parameters ["comp"] == null; }
		}


		internal const string blobEndpointSuffix = ".blob.core.windows.net";
		internal const string secondaryEndpointSuffix = "-secondary";

		public string GetAccountName(){
			if (!Address.Host.EndsWith(blobEndpointSuffix)){
				throw new InvalidOperationException(string.Format("Used blob storage endpoint is not default endpoint (*.{0}), cannot get account name", blobEndpointSuffix));
			}
			var name = Address.Host;
			var suffices = new string[]{blobEndpointSuffix, secondaryEndpointSuffix};
			foreach (var suffix in suffices){
				if (name.EndsWith(suffix)){
					name = name.Remove(name.Length - suffix.Length);
				}
			}
			return name;
		}

		#region Container operations

		public string GetContainerName ()
		{
			// If uri scheme is like http://hostname/blobname, container name is $root, otherwise first segment
			if (!IsContainerRef && Address.AbsolutePath.Count (c => c == '/') == 1) {
				return RootContainer;
			}
			
			// return first segment
			return Address.AbsolutePath.Split ('/') [1];
		}

		public bool IsListContainersOp {
			get{ return !IsContainerRef && !IsBlobRef && Parameters ["comp"] == "list"; }
		}

		public bool IsContainerListBlobsOp {
			get{ return IsContainerRef && Parameters ["comp"] == "list"; }
		}

		#endregion

		#region Blob operations

		/// <exception cref="InvalidOperationException">blob name does not exist on non-blob resource</exception>
		public string GetBlobName ()
		{
			if (IsContainerRef) {
				throw new InvalidOperationException (string.Format ("Cannot retrieve blob name on a container resource: {0}", Address));
			}
			var parts = Address.AbsolutePath.Split ('/');
			if (!IsBlobRef || parts [1] == "") {
				// non-blob resource on root level
				throw new InvalidOperationException (string.Format ("Cannot retrieve blob name on a non-blob resource: {0}", Address));
			}

			if (parts.Length == 2) {
				// Uri points to a blob in $root container e.g. "/myblob", return as is
				return parts [1];
			}

			// regular blob uri pattern, remove "/container/" part return the rest to the end (except query strings)
			return string.Join ("/", parts.Skip (2));
		}

		public bool IsBlockBlobOp {
			get{ return IsBlobRef && Parameters ["comp"] == "block" && Parameters ["blockid"] != null; }
		}

		public bool IsPageBlobOp {

			get{ return IsBlobRef && Parameters ["comp"] == "page"; }
		}

		public bool IsBlockListOp {
			get {
				return IsBlobRef && Parameters ["comp"] == "blocklist";
			}
		}

		/// <summary>
		/// Retrieves blockid from the URL, in context of a block-related operation
		/// </summary>
		/// <exception cref="InvalidOperationException">if resource is not a blob</exception>
		public string GetBlockId ()
		{
			if (!IsBlobRef) {
				throw new InvalidOperationException (string.Format ("Cannot retrieve block id on a non-blob resource: {0}", Address));
			}
			if (!IsBlockBlobOp) {
				throw new InvalidOperationException (string.Format ("Cannot retrieve block id on a non-block operation Uri: {0}", Address));
			}
			return Parameters ["blockid"];
		}

		#endregion
	}
}
