using NUnit.Framework;
using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using BlobMetaDb.Store.Redis;
using BookSleeve;

namespace BlobMetaDb.Store.Redis.Test
{
	[TestFixture]
	public class RedisBlobMetadataStoreTest
	{
		RedisConnection redis;
		RedisBlobMetadataStore store;
		internal const int db = 12;
		internal const string cntName = "cnt";
		internal const string blobName = "blob.txt";
		internal const string acct = "myaccount";

		internal string NewGuid ()
		{
			return Guid.NewGuid ().ToString ("N");
		}

		[TestFixtureSetUp]
		public void InitializeConnection ()
		{
			redis = new RedisConnection ("localhost", allowAdmin: true);
			redis.Open ().Wait ();
			store = new RedisBlobMetadataStore (redis, db);
		}

		[TestFixtureTearDown]
		[SetUp]
		public void ResetData ()
		{
			Console.WriteLine ("cleaned");
			redis.Server.FlushDb (db).Wait ();
		}

		[Test]
		public void CreateContainer ()
		{
			var cnt = NewGuid ();
			store.CreateContainerAsync (acct, cnt).Wait ();
			Assert.IsTrue (store.ContainerExistsAsync (acct, cnt).Result);
		}

		[Test]
		public void ContainerExists ()
		{
			Assert.IsFalse (store.ContainerExistsAsync (acct, cntName).Result);
			Assert.IsFalse (store.ContainerExistsAsync (acct, NewGuid ()).Result);
		}

		[Test]
		public void  DeleteContainer ()
		{
			var cnt = NewGuid ();
			store.CreateContainerAsync (acct, cnt).Wait ();
			store.DeleteContainerAsync (acct, cnt).Wait ();
			Assert.IsFalse (store.ContainerExistsAsync (acct, cnt).Result);
		}

		[Test]
		public void DeleteNonExistingContainer ()
		{
			store.DeleteContainerAsync (acct, NewGuid ()).Wait ();
		}

		[Test]
		public void CreateBlob ()
		{
			store.PutBlobAsync (acct, cntName, blobName).Wait ();
			Assert.IsTrue (store.BlobExistsAsync (acct, cntName, blobName).Result);

			var blobs = store.ListBlobsAsync (acct, cntName, false).Result;
			Assert.IsTrue (blobs.Any (b => b.Name == blobName));
		}

		[Test]
		public void CreateBlobUpdateSize ()
		{
			store.PutBlobAsync (acct, cntName, blobName).Wait ();
			var blobs = store.ListBlobsAsync (acct, cntName, true).Result.ToArray();
			Assert.IsNotEmpty (blobs);
			var blobMeta = blobs.First ();
			Assert.AreEqual (cntName, blobMeta.Container);
			Assert.AreEqual (blobName, blobMeta.Name);
			Assert.IsFalse (blobMeta.Size.HasValue);

			// Update blob
			long size = long.MaxValue;
			store.PutBlobAsync (acct, cntName, blobName, size).Wait ();

			blobs = store.ListBlobsAsync (acct, cntName, true).Result.ToArray();
			Assert.IsNotEmpty (blobs);
			blobMeta = blobs.First ();
			Assert.IsTrue (blobMeta.Size.HasValue);
			Assert.AreEqual (size, blobMeta.Size.Value);
		}

		[Test]
		public void DeleteBlob ()
		{
			store.PutBlobAsync (acct, cntName, blobName).Wait ();
			store.DeleteBlobAsync (acct, cntName, blobName).Wait ();
			Assert.IsFalse (store.BlobExistsAsync (acct, cntName, blobName).Result);

			var blobs = store.ListBlobsAsync (acct, cntName, false).Result;
			Assert.IsFalse (blobs.Any (b => b.Name == blobName));
		}

		[Test]
		public void DeleteNonExistingBlob ()
		{
			store.DeleteBlobAsync (acct, NewGuid (), NewGuid ()).Wait ();
		}

		[Test]
		public void BlobExists ()
		{
			Assert.IsFalse (store.BlobExistsAsync (acct, NewGuid (), NewGuid ()).Result);
			string cnt = NewGuid (), blob = NewGuid ();
			store.PutBlobAsync (acct, cnt, blob).Wait ();
			Assert.IsTrue (store.BlobExistsAsync (acct, cnt, blob).Result);
		}

		[Test]
		public void CreateContainerCleansBlobs ()
		{
			store.CreateContainerAsync (acct, cntName).Wait ();

			int n = 100;
			Task[] tasks = new Task[n];
			string[] blobs = new string[n];
			for (int i = 0; i < n; i++) {
				blobs [i] = NewGuid () + ".txt";
				tasks [i] = store.PutBlobAsync (acct, cntName, blobs [i]);
			}
			Task.WaitAll (tasks);

			store.CreateContainerAsync (acct, cntName).Wait ();

			// At this point redis should only contain "containers" and "container:cnt"
			Assert.AreEqual (2, redis.Keys.Find (db, "*").Result.Length);

			Assert.IsEmpty (store.ListBlobsAsync (acct, cntName, false).Result.ToArray());
			foreach (string blob in blobs) {
				Assert.IsFalse (store.BlobExistsAsync (acct, cntName, blob).Result);
			}

			store.DeleteContainerAsync (acct, cntName).Wait ();

			// At this point redis should be empty.
			Assert.IsEmpty (redis.Keys.Find (db, "*").Result);
		}

		[Test]
		public void ListBlobs ()
		{
			// list blobs on non-existing container
			Assert.IsEmpty (store.ListBlobsAsync (acct, cntName, false).Result.ToArray());

			// list blobs on non-empty container
			store.CreateContainerAsync (acct, cntName).Wait ();
			Assert.IsEmpty (store.ListBlobsAsync (acct, cntName, false).Result.ToArray());
		}

		[Test]
		public void ListContainers ()
		{
			Assert.IsEmpty (store.ListContainersAsync (acct).Result.ToArray());

			int n = 100;
			for (int i = 0; i < n; i++) {
				store.CreateContainerAsync (acct, cntName + i.ToString ()).Wait ();
			}
			Assert.AreEqual (n, store.ListContainersAsync (acct).Result.Count ());
		}

		[Test]
		public void CreateContainerIfNotExists ()
		{
			store.CreateContainerIfNotExistsAsync (acct, cntName).Wait ();
			store.PutBlobAsync (acct, cntName, blobName);
			store.CreateContainerIfNotExistsAsync (acct, cntName).Wait ();
			Assert.AreEqual (1, store.ListContainersAsync (acct).Result.Count ());
			Assert.AreEqual (1, store.ListBlobsAsync (acct, cntName, false).Result.Count ());
		}

		[Test]
		public void	DeleteContainerCleansBlobs ()
		{
			store.CreateContainerAsync (acct, cntName).Wait ();
			store.PutBlobAsync (acct, cntName, blobName).Wait ();
			store.DeleteContainerAsync (acct, cntName).Wait ();
			Assert.AreEqual (0, store.ListBlobsAsync (acct, cntName, false).Result.Count ());
			Assert.AreEqual (0, redis.Keys.Find (0, "*").Result.Length);
		}
	}
}

