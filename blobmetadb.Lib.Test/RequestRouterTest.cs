using NUnit.Framework;
using System;
using BlobMetaDb.Lib;

namespace BlobMetaDb.Lib.Test
{
	/// <summary>
	/// Verifies what API calls are triggering what metadatastore operations.
	/// This class ideally should contain all kinds of REST HTTP calls in Blob Storage API.
	/// </summary>
	[TestFixture]
	public class RequestRouterTest
	{
		internal static readonly string GET = "GET";
		internal static readonly string POST = "POST";
		internal static readonly string PUT = "PUT";
		internal static readonly string DELETE = "DELETE";
		internal static readonly string HEAD = "HEAD";
		internal static InMemoryBlobMetadataStore Store = new InMemoryBlobMetadataStore ();

		internal RequestRouter RouterFor (DiscoveryMode mode, string method, string uri)
		{
			Store = new InMemoryBlobMetadataStore ();
			return new RequestRouter (mode, Store, method, new Uri (uri));
		}

		/// <summary>
		/// Makes sure the operation is not handled and no calls were made to IBlobStorageMetadataStore.
		/// </summary>
		internal void AssertUnhandledRequest (RequestRouter router)
		{
			Assert.IsFalse (router.RouteAsync ().Result);
			Assert.AreEqual (MetadataStoreOperation.Undefined, Store.LastOperation);
		}

		#region Operations on the Account (Blob Service) (msdn.microsoft.com/en-us/library/windowsazure/dd135724.aspx)

		[Test]
		public void ListContainers ()
		{
			foreach (var mode in Enum.GetValues(typeof(DiscoveryMode))) {
				AssertUnhandledRequest (RouterFor ((DiscoveryMode)mode, GET, "https://myaccount.blob.core.windows.net/?comp=list"));
			}
		}

		[Test]
		public void SetBlobServiceProperties ()
		{
			foreach (var mode in Enum.GetValues(typeof(DiscoveryMode))) {
				AssertUnhandledRequest (RouterFor ((DiscoveryMode)mode, PUT, "https://myaccount.blob.core.windows.net/?restype=service&comp=properties"));
			}
		}

		[Test]
		public void GetBlobServiceProperties ()
		{
			foreach (var mode in Enum.GetValues(typeof(DiscoveryMode))) {
				AssertUnhandledRequest (RouterFor ((DiscoveryMode)mode, GET, "https://myaccount.blob.core.windows.net/?restype=service&comp=properties"));
			}
		}

		[Test]
		public void PreflightBlobRequest ()
		{
			string OPTIONS = "OPTIONS", url = "https://myaccount.blob.core.windows.net/mycontainer/myblockblob";

			AssertUnhandledRequest (RouterFor (DiscoveryMode.OnlyWrites, OPTIONS, url));

			foreach (var mode in new[]{DiscoveryMode.AllRequests, DiscoveryMode.AllRequestsEnsureSchema}) {
				var router = RouterFor (mode, OPTIONS, url);
				Assert.IsTrue (router.RouteAsync ().Result);

				var expected = (mode == DiscoveryMode.AllRequests) ? MetadataStoreOperation.PutBlob : (MetadataStoreOperation.CreateContainerIfNotExists | MetadataStoreOperation.PutBlob);
				Assert.AreEqual (expected, Store.LastOperation);
			}
		}

		[Test]
		public void GetBlobServiceStats ()
		{
			foreach (var mode in Enum.GetValues(typeof(DiscoveryMode))) {
				foreach (var method in new[]{GET,HEAD}) {
					AssertUnhandledRequest (RouterFor ((DiscoveryMode)mode, method, "https://myaccount-secondary.blob.core.windows.net/?restype=service&comp=stats"));
				}
			}
		}

		#endregion

		#region Operations on Containers (msdn.microsoft.com/en-us/library/windowsazure/dd179468.aspx)

		[Test]
		public void GetContainerProperties ()
		{
			string url = "https://myaccount.blob.core.windows.net/mycontainer?restype=container";
			foreach (var method in new[]{GET,HEAD}) {
				AssertUnhandledRequest (RouterFor (DiscoveryMode.OnlyWrites, method, url));

				foreach (var mode in new[]{DiscoveryMode.AllRequests, DiscoveryMode.AllRequestsEnsureSchema}) {
					var router = RouterFor (mode, method, url);
					Assert.IsTrue (router.RouteAsync ().Result);
					Assert.AreEqual (MetadataStoreOperation.CreateContainerIfNotExists, Store.LastOperation);
				}
			}
			
		}

		[Test]
		public void GetContainerMetadata ()
		{
			string url = "https://myaccount.blob.core.windows.net/mycontainer?restype=container&comp=metadata";

			foreach (var method in new[]{GET,HEAD}) {
				AssertUnhandledRequest (RouterFor (DiscoveryMode.OnlyWrites, method, url));

				foreach (var mode in new[]{DiscoveryMode.AllRequests, DiscoveryMode.AllRequestsEnsureSchema}) {
					var router = RouterFor (mode, method, url);
					Assert.IsTrue (router.RouteAsync ().Result);
					Assert.AreEqual (MetadataStoreOperation.CreateContainerIfNotExists, Store.LastOperation);
				}
			}
		}

		[Test]
		public void SetContainerMetadata ()
		{
			string url = "https://myaccount.blob.core.windows.net/mycontainer?restype=container&comp=metadata";

			AssertUnhandledRequest (RouterFor (DiscoveryMode.OnlyWrites, PUT, url));

			foreach (var mode in new[]{DiscoveryMode.AllRequests, DiscoveryMode.AllRequestsEnsureSchema}) {
				var router = RouterFor (mode, PUT, url);
				Assert.IsTrue (router.RouteAsync ().Result);
				Assert.AreEqual (MetadataStoreOperation.CreateContainerIfNotExists, Store.LastOperation);
			}
		}

		[Test]
		public void GetContainerAcl ()
		{
			string url = "https://myaccount.blob.core.windows.net/mycontainer?restype=container&comp=acl";

			foreach (var method in new[]{GET,HEAD}) {
				AssertUnhandledRequest (RouterFor (DiscoveryMode.OnlyWrites, method, url));

				foreach (var mode in new[]{DiscoveryMode.AllRequests, DiscoveryMode.AllRequestsEnsureSchema}) {
					var router = RouterFor (mode, method, url);
					Assert.IsTrue (router.RouteAsync ().Result);
					Assert.AreEqual (MetadataStoreOperation.CreateContainerIfNotExists, Store.LastOperation);
				}
			}
		}

		[Test]
		public void SetContainerAcl ()
		{
			string url = "https://myaccount.blob.core.windows.net/mycontainer?restype=container&comp=acl";

			AssertUnhandledRequest (RouterFor (DiscoveryMode.OnlyWrites, PUT, url));

			foreach (var mode in new[]{DiscoveryMode.AllRequests, DiscoveryMode.AllRequestsEnsureSchema}) {
				var router = RouterFor (mode, PUT, url);
				Assert.IsTrue (router.RouteAsync ().Result);
				Assert.AreEqual (MetadataStoreOperation.CreateContainerIfNotExists, Store.LastOperation);
			}
		}

		[Test]
		public void LeaseContainer ()
		{
			string url = "https://myaccount.blob.core.windows.net/mycontainer?restype=container&comp=lease";

			AssertUnhandledRequest (RouterFor (DiscoveryMode.OnlyWrites, PUT, url));

			foreach (var mode in new[]{DiscoveryMode.AllRequests, DiscoveryMode.AllRequestsEnsureSchema}) {
				var router = RouterFor (mode, PUT, url);
				Assert.IsTrue (router.RouteAsync ().Result);
				Assert.AreEqual (MetadataStoreOperation.CreateContainerIfNotExists, Store.LastOperation);
			}
		}

		[Test]
		public void CreateContainer ()
		{
			foreach (var mode in Enum.GetValues(typeof(DiscoveryMode))) {
				var router = RouterFor ((DiscoveryMode)mode, PUT, "https://myaccount.blob.core.windows.net/mycontainer?restype=container");
				Assert.IsTrue (router.RouteAsync ().Result);
				Assert.AreEqual (MetadataStoreOperation.CreateContainer, Store.LastOperation);
			}
		}

		[Test]
		public void DeleteContainer ()
		{
			foreach (var mode in Enum.GetValues(typeof(DiscoveryMode))) {
				var router = RouterFor ((DiscoveryMode)mode, DELETE, "https://myaccount.blob.core.windows.net/mycontainer?restype=container");
				Assert.IsTrue (router.RouteAsync ().Result);
				Assert.AreEqual (MetadataStoreOperation.DeleteContainer, Store.LastOperation);
			}
		}

		[Test]
		public void ListBlobs ()
		{
			string url = "https://myaccount.blob.core.windows.net/mycontainer?restype=container&comp=list&delimiter=/";

			AssertUnhandledRequest (RouterFor (DiscoveryMode.OnlyWrites, GET, url));

			foreach (var mode in new[]{DiscoveryMode.AllRequests, DiscoveryMode.AllRequestsEnsureSchema}) {
				var router = RouterFor (mode, GET, url);
				Assert.IsTrue (router.RouteAsync ().Result);
				Assert.AreEqual (MetadataStoreOperation.CreateContainerIfNotExists, Store.LastOperation);
			}
		}

		#endregion

		#region Operations on Blobs – Generic (msdn.microsoft.com/en-us/library/windowsazure/dd179377.aspx)

		[Test]
		public void PutBlob ()
		{
			foreach (var mode in Enum.GetValues(typeof(DiscoveryMode))) {
				var router = RouterFor ((DiscoveryMode)mode, PUT, "https://myaccount.blob.core.windows.net/mycontainer/myblob");
				Assert.IsTrue (router.RouteAsync ().Result);
				var expected = ((DiscoveryMode)mode == DiscoveryMode.AllRequestsEnsureSchema) ? (MetadataStoreOperation.CreateContainerIfNotExists | MetadataStoreOperation.PutBlob) : MetadataStoreOperation.PutBlob;
				Assert.AreEqual (expected, Store.LastOperation);
			}
		}

		[Test]
		public void GetBlob ()
		{
			string url = "https://myaccount.blob.core.windows.net/mycontainer/myblob";

			AssertUnhandledRequest (RouterFor (DiscoveryMode.OnlyWrites, GET, url));

			foreach (var mode in new[]{DiscoveryMode.AllRequests, DiscoveryMode.AllRequestsEnsureSchema}) {
				var router = RouterFor ((DiscoveryMode)mode, GET, url);
				Assert.IsTrue (router.RouteAsync ().Result);
				var expected = (mode == DiscoveryMode.AllRequests) ? MetadataStoreOperation.PutBlob : (MetadataStoreOperation.CreateContainerIfNotExists | MetadataStoreOperation.PutBlob);
				Assert.AreEqual (expected, Store.LastOperation);
			}
		}

		[Test]
		public void GetBlobProperties ()
		{
			string url = "https://myaccount.blob.core.windows.net/mycontainer/myblob";

			AssertUnhandledRequest (RouterFor (DiscoveryMode.OnlyWrites, HEAD, url));

			foreach (var mode in new[]{DiscoveryMode.AllRequests, DiscoveryMode.AllRequestsEnsureSchema}) {
				var router = RouterFor ((DiscoveryMode)mode, HEAD, url);
				Assert.IsTrue (router.RouteAsync ().Result);
				var expected = (mode == DiscoveryMode.AllRequests) ? MetadataStoreOperation.PutBlob : (MetadataStoreOperation.CreateContainerIfNotExists | MetadataStoreOperation.PutBlob);
				Assert.AreEqual (expected, Store.LastOperation);
			}
		}

		[Test]
		public void SetBlobProperties ()
		{
			string url = "https://myaccount.blob.core.windows.net/mycontainer/myblob?comp=properties";
			AssertUnhandledRequest (RouterFor (DiscoveryMode.OnlyWrites, PUT, url));

			foreach (var mode in new[]{DiscoveryMode.AllRequests, DiscoveryMode.AllRequestsEnsureSchema}) {
				var router = RouterFor ((DiscoveryMode)mode, PUT, url);
				Assert.IsTrue (router.RouteAsync ().Result);
				var expected = (mode == DiscoveryMode.AllRequests) ? MetadataStoreOperation.PutBlob : (MetadataStoreOperation.CreateContainerIfNotExists | MetadataStoreOperation.PutBlob);
				Assert.AreEqual (expected, Store.LastOperation);
			}
		}

		[Test]
		public void GetBlobMetadata ()
		{
			string url = "https://myaccount.blob.core.windows.net/mycontainer/myblob?comp=metadata";

			foreach (var method in new[]{GET,HEAD}) {
				AssertUnhandledRequest (RouterFor (DiscoveryMode.OnlyWrites, method, url));
				foreach (var mode in new[]{DiscoveryMode.AllRequests, DiscoveryMode.AllRequestsEnsureSchema}) {
					var router = RouterFor ((DiscoveryMode)mode, method, url);
					Assert.IsTrue (router.RouteAsync ().Result);
					var expected = (mode == DiscoveryMode.AllRequests) ? MetadataStoreOperation.PutBlob : (MetadataStoreOperation.CreateContainerIfNotExists | MetadataStoreOperation.PutBlob);
					Assert.AreEqual (expected, Store.LastOperation);
				}
			}
		}

		[Test]
		public void SetBlobMetadata ()
		{
			string url = "https://myaccount.blob.core.windows.net/mycontainer/myblob?comp=metadata";

			AssertUnhandledRequest (RouterFor (DiscoveryMode.OnlyWrites, PUT, url));

			foreach (var mode in new[]{DiscoveryMode.AllRequests, DiscoveryMode.AllRequestsEnsureSchema}) {
				var router = RouterFor ((DiscoveryMode)mode, PUT, url);
				Assert.IsTrue (router.RouteAsync ().Result);
				var expected = (mode == DiscoveryMode.AllRequests) ? MetadataStoreOperation.PutBlob : (MetadataStoreOperation.CreateContainerIfNotExists | MetadataStoreOperation.PutBlob);
				Assert.AreEqual (expected, Store.LastOperation);
			}
		}

		[Test]
		public void LeaseBlob ()
		{
			string url = "https://myaccount.blob.core.windows.net/mycontainer/myblob?comp=lease";

			AssertUnhandledRequest (RouterFor (DiscoveryMode.OnlyWrites, PUT, url));

			foreach (var mode in new[]{DiscoveryMode.AllRequests, DiscoveryMode.AllRequestsEnsureSchema}) {
				var router = RouterFor ((DiscoveryMode)mode, PUT, url);
				Assert.IsTrue (router.RouteAsync ().Result);
				var expected = (mode == DiscoveryMode.AllRequests) ? MetadataStoreOperation.PutBlob : (MetadataStoreOperation.CreateContainerIfNotExists | MetadataStoreOperation.PutBlob);
				Assert.AreEqual (expected, Store.LastOperation);
			}
		}

		[Test]
		public void SnapshotBlob ()
		{
			string url = "https://myaccount.blob.core.windows.net/mycontainer/myblob?comp=snapshot";

			AssertUnhandledRequest (RouterFor (DiscoveryMode.OnlyWrites, PUT, url));

			foreach (var mode in new[]{DiscoveryMode.AllRequests, DiscoveryMode.AllRequestsEnsureSchema}) {
				var router = RouterFor ((DiscoveryMode)mode, PUT, url);
				Assert.IsTrue (router.RouteAsync ().Result);
				var expected = ((DiscoveryMode)mode == DiscoveryMode.AllRequestsEnsureSchema) ? (MetadataStoreOperation.CreateContainerIfNotExists | MetadataStoreOperation.PutBlob) : MetadataStoreOperation.PutBlob;
				Assert.AreEqual (expected, Store.LastOperation);
			}
		}

		[Test]
		public void CopyBlob ()
		{
			foreach (var mode in Enum.GetValues(typeof(DiscoveryMode))) {
				var router = RouterFor ((DiscoveryMode)mode, PUT, "https://myaccount.blob.core.windows.net/mycontainer/myblob");//TODO pass headers to router to show copy.
				Assert.IsTrue (router.RouteAsync ().Result);

				var expected = ((DiscoveryMode)mode == DiscoveryMode.AllRequestsEnsureSchema) ? (MetadataStoreOperation.CreateContainerIfNotExists | MetadataStoreOperation.PutBlob) : MetadataStoreOperation.PutBlob;
				Assert.AreEqual (expected, Store.LastOperation);
			}
		}

		[Test]
		public void AbortCopyBlob ()
		{
			foreach (var mode in Enum.GetValues(typeof(DiscoveryMode))) {
				var router = RouterFor ((DiscoveryMode)mode, PUT, "https://myaccount.blob.core.windows.net/mycontainer/myblobcomp=copy&copyid=ID");
				Assert.IsTrue (router.RouteAsync ().Result);
				var expected = ((DiscoveryMode)mode == DiscoveryMode.AllRequestsEnsureSchema) ? (MetadataStoreOperation.CreateContainerIfNotExists | MetadataStoreOperation.PutBlob) : MetadataStoreOperation.PutBlob;
				Assert.AreEqual (expected, Store.LastOperation);
			}
		}

		public void DeleteBlob ()
		{
			foreach (var mode in Enum.GetValues(typeof(DiscoveryMode))) {
				var router = RouterFor ((DiscoveryMode)mode, DELETE, "https://myaccount.blob.core.windows.net/mycontainer/myblob");
				Assert.IsTrue (router.RouteAsync ().Result);
				var expected = ((DiscoveryMode)mode == DiscoveryMode.AllRequests) ? MetadataStoreOperation.PutBlob : (MetadataStoreOperation.CreateContainerIfNotExists | MetadataStoreOperation.PutBlob);
				Assert.AreEqual (expected, Store.LastOperation);
			}
		}

		#endregion

		#region Operations on Block Blobs (msdn.microsoft.com/en-us/library/windowsazure/ee691974.aspx)

		[Test]
		public void PutBlock ()
		{
			string url = "https://myaccount.blob.core.windows.net/mycontainer/myblob?comp=block&blockid=QUFB";
			AssertUnhandledRequest (RouterFor (DiscoveryMode.OnlyWrites, PUT, url));

			foreach (var m in Enum.GetValues(typeof(DiscoveryMode))) {
				var mode = (DiscoveryMode)m;
				Console.WriteLine (mode);
				var router = RouterFor (mode, PUT, url);

				if (mode == DiscoveryMode.AllRequestsEnsureSchema) {
					Assert.IsTrue (router.RouteAsync ().Result);
					Assert.AreEqual (MetadataStoreOperation.CreateContainerIfNotExists, Store.LastOperation);
				} else {
					AssertUnhandledRequest (router);
				}
			}
		}

		[Test]
		public void PutBlockList ()
		{
			foreach (var mode in Enum.GetValues(typeof(DiscoveryMode))) {
				var router = RouterFor ((DiscoveryMode)mode, PUT, "https://myaccount.blob.core.windows.net/mycontainer/myblob?comp=blocklist");
				Assert.IsTrue (router.RouteAsync ().Result);
				var expected = ((DiscoveryMode)mode == DiscoveryMode.AllRequestsEnsureSchema) ? (MetadataStoreOperation.CreateContainerIfNotExists | MetadataStoreOperation.PutBlob) : MetadataStoreOperation.PutBlob;
				Assert.AreEqual (expected, Store.LastOperation);
			}
		}

		[Test]
		public void GetBlockList ()
		{
			string url = "https://myaccount.blob.core.windows.net/mycontainer/myblob?comp=blocklist";

			AssertUnhandledRequest (RouterFor (DiscoveryMode.OnlyWrites, GET, url));

			foreach (var mode in new[]{DiscoveryMode.AllRequests, DiscoveryMode.AllRequestsEnsureSchema}) {
				var router = RouterFor (mode, GET, url);
				Assert.IsTrue (router.RouteAsync ().Result);
				var expected = (mode == DiscoveryMode.AllRequests) ? MetadataStoreOperation.PutBlob : (MetadataStoreOperation.CreateContainerIfNotExists | MetadataStoreOperation.PutBlob);
				Assert.AreEqual (expected, Store.LastOperation);
			}
		}

		#endregion

		#region Operations on Page Blobs (msdn.microsoft.com/en-us/library/windowsazure/ee691970.aspx)

		[Test]
		public void PutPage ()
		{
			string url = "https://myaccount.blob.core.windows.net/mycontainer/myblob?comp=page";

			AssertUnhandledRequest (RouterFor (DiscoveryMode.OnlyWrites, PUT, url));

			foreach (var mode in new[]{DiscoveryMode.AllRequests, DiscoveryMode.AllRequestsEnsureSchema}) {
				var router = RouterFor (mode, PUT, url);
				Assert.IsTrue (router.RouteAsync ().Result);
				var expected = (mode == DiscoveryMode.AllRequests) ? MetadataStoreOperation.PutBlob : (MetadataStoreOperation.CreateContainerIfNotExists | MetadataStoreOperation.PutBlob);
				Assert.AreEqual (expected, Store.LastOperation);
			}
		}

		[Test]
		public void GetPageRanges ()
		{
			string url = "https://myaccount.blob.core.windows.net/mycontainer/myblob?comp=pagelist";

			AssertUnhandledRequest (RouterFor (DiscoveryMode.OnlyWrites, GET, url));

			foreach (var mode in new[]{DiscoveryMode.AllRequests, DiscoveryMode.AllRequestsEnsureSchema}) {
				var router = RouterFor (mode, GET, url);
				Assert.IsTrue (router.RouteAsync ().Result);
				var expected = (mode == DiscoveryMode.AllRequests) ? MetadataStoreOperation.PutBlob : (MetadataStoreOperation.CreateContainerIfNotExists | MetadataStoreOperation.PutBlob);
				Assert.AreEqual (expected, Store.LastOperation);
			}
		}

		#endregion
	}
}
