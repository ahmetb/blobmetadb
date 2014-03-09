using NUnit.Framework;
using System;
using BlobMetaDb.Lib;

namespace BlobMetaDb.Lib.Test
{
	[TestFixture]
	public class UriResolverTest
	{
		internal const string RootContainer = "$root";

		internal static UrlResolver Resolve (string url)
		{
			return new UrlResolver (new Uri (url));
		}

		[Test]
		public void TestContainerUrls ()
		{
			var cntUrl = Resolve ("https://myaccount.blob.core.windows.net/mycontainer?restype=container");

			Assert.IsTrue (cntUrl.IsContainerRef);
			Assert.IsFalse (cntUrl.IsBlobRef);
			Assert.IsTrue (cntUrl.IsCompNone);
			Assert.AreEqual ("mycontainer", cntUrl.GetContainerName ());
			Assert.AreEqual ("myaccount", cntUrl.GetAccountName());

			var cntListBlobsUrl = Resolve ("https://myaccount-secondary.blob.core.windows.net/mycontainer?restype=container&comp=list");

			Assert.IsTrue (cntListBlobsUrl.IsContainerRef);
			Assert.IsFalse (cntListBlobsUrl.IsBlobRef);
			Assert.IsFalse (cntListBlobsUrl.IsCompNone);
			Assert.IsTrue (cntListBlobsUrl.IsContainerListBlobsOp);
			Assert.AreEqual ("myaccount", cntUrl.GetAccountName());
			Assert.AreNotEqual ("myaccount-secondary", cntUrl.GetAccountName());
		}

		[Test]
		public void TestBlockBlobOps ()
		{
			var aBlock = Resolve ("https://myaccount.blob.core.windows.net/mycontainer/myblob?comp=block&blockid=AAAA");
			Assert.IsFalse (aBlock.IsContainerRef);
			Assert.IsTrue (aBlock.IsBlobRef);
			Assert.IsTrue (aBlock.IsBlockBlobOp);
			Assert.AreEqual ("AAAA", aBlock.GetBlockId ());
			Assert.IsFalse (aBlock.IsBlockListOp);

			var blockList = Resolve ("https://myaccount.blob.core.windows.net/mycontainer/myblob?comp=blocklist");
			Assert.IsFalse (blockList.IsContainerRef);
			Assert.IsTrue (blockList.IsBlobRef);
			Assert.IsFalse (blockList.IsBlockBlobOp);
			Assert.IsTrue (blockList.IsBlockListOp);
		}

		[Test]
		public void TestPageBlobOps ()
		{
			var pageBlob = Resolve ("https://myaccount.blob.core.windows.net/mycontainer/myblob?comp=page");

			Assert.IsTrue (pageBlob.IsBlobRef);
			Assert.IsFalse (pageBlob.IsContainerRef);
			Assert.IsTrue (pageBlob.IsPageBlobOp);
			Assert.IsFalse (pageBlob.IsBlockBlobOp);
		}

		[Test]
		public void TestCommonBlobOps ()
		{
			var blob = Resolve ("https://myaccount.blob.core.windows.net/mycontainer/myblob");
			Assert.IsTrue (blob.IsBlobRef);
			Assert.IsFalse (blob.IsContainerRef);
			Assert.AreEqual ("mycontainer",blob.GetContainerName ());
			Assert.AreEqual ("myblob", blob.GetBlobName ());
			Assert.IsFalse (blob.IsPageBlobOp);
			Assert.IsFalse (blob.IsBlockBlobOp);

			var nestedBlob = Resolve ("https://myaccount.blob.core.windows.net/mycontainer/mynestedcontainer/myblob");
			Assert.AreEqual ("mycontainer", nestedBlob.GetContainerName ());
			Assert.AreEqual ("mynestedcontainer/myblob", nestedBlob.GetBlobName ());
		}

		[Test]
		public void TestAccountOps ()
		{
			// Test "list containers" op.
			var blob = Resolve ("https://myaccount.blob.core.windows.net/?comp=list");

			Assert.IsTrue (blob.IsListContainersOp);
			Assert.IsFalse (blob.IsBlobRef);
			Assert.IsFalse (blob.IsContainerRef);
			Assert.IsFalse (blob.IsContainerListBlobsOp);
		}

		[Test]
		public void TestRootContainerUris ()
		{
			var blobOnRoot = Resolve ("https://myaccount.blob.core.windows.net/myphoto?comp=metadata");
			Assert.IsTrue (blobOnRoot.IsBlobRef);
			Assert.IsFalse (blobOnRoot.IsContainerRef);
			Assert.AreEqual ("myphoto", blobOnRoot.GetBlobName ());
			Assert.AreEqual (RootContainer, blobOnRoot.GetContainerName ());

			var blobOnRootExplicit = Resolve ("https://myaccount.blob.core.windows.net/$root/myphoto?comp=metadata");
			Assert.IsTrue (blobOnRootExplicit.IsBlobRef);
			Assert.IsFalse (blobOnRootExplicit.IsContainerRef);
			Assert.AreEqual ("myphoto", blobOnRootExplicit.GetBlobName ());
			Assert.AreEqual (RootContainer, blobOnRootExplicit.GetContainerName ());

			var rootContainer = Resolve ("https://myaccount.blob.core.windows.net/$root?restype=container");
			Assert.IsFalse (rootContainer.IsBlobRef);
			Assert.IsTrue (rootContainer.IsContainerRef);
			Assert.AreEqual (RootContainer, rootContainer.GetContainerName ());
		}
	}
}

