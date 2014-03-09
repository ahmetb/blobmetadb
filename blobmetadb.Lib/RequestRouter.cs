using System;
using System.Threading.Tasks;
using System.Net;
using System.Runtime.CompilerServices;

namespace BlobMetaDb.Lib
{
	/// <summary>

	/// Helps understanding what to do with Blob Storage requests completed.
	/// </summary>
	public class RequestRouter
	{
		internal static readonly string GET = "GET";
		internal static readonly string POST = "POST";
		internal static readonly string PUT = "PUT";
		internal static readonly string DELETE = "DELETE";
		internal static readonly string HEAD = "HEAD";
		internal static readonly string BlobSizeHeader = "x-ms-blob-content-length";

		private DiscoveryMode Mode { get; set; }

		private string method { get; set; }

		private IBlobStorageMetadataStore store { get; set; }

		private UrlResolver resolver { get; set; }

		private HttpWebRequest request { get; set; }

		// do not read response stream!
		private HttpWebResponse response { get; set; }


		/// <summary>
		/// Initializes a new instance of the <see cref="BlobMetaDb.Lib.RequestRouter"/> class with given
		/// blob storage request instance that completed successfully and a metadata storage to forward
		/// those requests.
		/// </summary>
		public RequestRouter (DiscoveryMode mode, IBlobStorageMetadataStore store, string requestMethod, Uri requestUri)
		{
			this.method = requestMethod;
			this.Mode = mode;
			this.store = store;
			this.resolver = new UrlResolver (requestUri);
		}

		/// <summary>
		/// Alternative constructor to <see cref="RequestRouter"/> that could be called from a 
		/// </summary>
		public RequestRouter (DiscoveryMode mode, IBlobStorageMetadataStore store, HttpWebRequest request, HttpWebResponse response) :
			this (mode, store, request.Method, request.Address)
		{
			this.request = request;
			this.response = response;
		}

		#region Helper methods

		#endregion

		/// <summary>
		/// Routes given Write request to the datastore once.
		/// </summary>
		/// <returns>true if handled and action taken, false if not</returns>
		public async Task<bool> RouteAsync ()
		{
			bool handled = false;

			if (resolver.IsBlobRef) {
				handled = await HandleBlobOperationAsync ();
			} else if (resolver.IsContainerRef) {
				handled = await HandleContainerOperationAsync ();
			}
			return handled;
		}

		#region Handlers for resource types

		internal async Task<bool> HandleContainerOperationAsync ()
		{
			if (IsCreateContainerOperation) {
				await store.CreateContainerAsync (resolver.GetAccountName (), resolver.GetContainerName ());
				return true;
			} else if (IsDeleteContainerOperation) {
				await store.DeleteContainerAsync (resolver.GetAccountName (), resolver.GetContainerName ());
				return true;
			} else if (Mode != DiscoveryMode.OnlyWrites && IsImplyingResourceExists) {
				// Discovered container through GET/HEAD/PUT request on container,
				// current mode requires datastore to be informed about discovery.
				await store.CreateContainerIfNotExistsAsync (resolver.GetAccountName (), resolver.GetContainerName ());
				return true;
			}

			return false;
		}

		internal async Task<bool> HandleBlobOperationAsync ()
		{
			var handled = false;

			if (Mode == DiscoveryMode.AllRequestsEnsureSchema) {
				// Ensure container exists first.
				await store.CreateContainerIfNotExistsAsync (resolver.GetAccountName (), resolver.GetContainerName ());
				handled = true;
			}

			long? blobSize;
			if (TryGetBlobSize (out blobSize)) {
				Console.ForegroundColor = ConsoleColor.DarkYellow;
				Console.WriteLine ("Found blob of size: " + blobSize);
				Console.ResetColor();
			}

			if (IsDeleteBlobOperation) {
				await store.DeleteBlobAsync (resolver.GetAccountName (), resolver.GetContainerName (), resolver.GetBlobName ());
				return true;
			} else if (IsPutBlobOperation || IsPutBlockListOperation) {
				await store.PutBlobAsync (resolver.GetAccountName (), resolver.GetContainerName (), resolver.GetBlobName (), blobSize);
				return true;
			} else if (IsImplyingResourceExists) {
				if (Mode != DiscoveryMode.OnlyWrites) {
					// Discovered blob through GET/HEAD/PUT requests on blob,
					// current mode requires datastore to be informed about discovery.
					await store.PutBlobAsync (resolver.GetAccountName (), resolver.GetContainerName (), resolver.GetBlobName (), blobSize);
					return true;
				}
			}
			return handled;
		}

		#endregion

		#region Determine blob operations (called only when resolver.IsBlobRef == true)

		internal bool IsDeleteBlobOperation {
			get { return method == DELETE && resolver.IsCompNone; }
		}

		internal bool IsPutBlockOperation {
			get { return method == PUT && resolver.IsBlockBlobOp; }
		}

		internal bool IsPutBlockListOperation {
			get { return method == PUT && resolver.IsBlockListOp; }
		}

		internal bool IsPutPageOperation {
			get { return method == PUT && resolver.IsPageBlobOp; }
		}

		internal bool IsPutBlobOperation {
			get { return method == PUT && resolver.IsCompNone; }
		}
		internal bool IsGetBlobOperation {
			get { return (method == GET || method == HEAD) && resolver.IsCompNone; }
		}
		//TODO copy blob can fetch blob size from other blob (if exists) (x-ms-copy-source header?)

		#endregion

		#region Determine container operations (called only when resolver.IsContainerRef == true)

		internal bool IsCreateContainerOperation {
			get { return method == PUT && resolver.IsCompNone; }
		}

		internal bool IsDeleteContainerOperation {
			get { return method == DELETE; }
		}

		#endregion

		internal bool TryGetBlobSize(out long? size){
			if (IsPutBlobOperation && request != null) {
				// Other requests like "Put Block List" may be misleading.

				// try reading "x-ms-blob-content-length" if exists.
				var blobSizeHeader = request.Headers.Get(BlobSizeHeader);
				if (blobSizeHeader != null){
					size = long.Parse(blobSizeHeader);
					return true;
				}
				// try obtaining Content-Length
				if (request.ContentLength != -1) {
					size = request.ContentLength;
					return true;
				}
			}

			if (IsGetBlobOperation && response != null) {
				// We can read data from Content-Length header of blob download (or peek) response.
				if (response.ContentLength != -1) {
					size = response.ContentLength;
					return true;
				}
			}
			// No other way to figure out
			size = null;
			return false;
		}

		internal bool IsImplyingResourceExists {
			get {
				if (method == DELETE) {
					return false;
				}

				// Put block operation on blob does not mean blob exists.
				if (resolver.IsBlobRef && method == PUT && resolver.IsBlockBlobOp) {
					return false;
				}
				// All other requests imply resource exists
				return true;
			}
		}
	}
}

